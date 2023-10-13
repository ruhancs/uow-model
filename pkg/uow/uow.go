package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// factory de repositorios
type RepositoryFactory func(tx *sql.Tx) any

type UowInterface interface {
	Register(name string, factory RepositoryFactory)
	GetRepository(ctx context.Context, name string) (any, error)
	Do(ctx context.Context, fn func(uow *Uow) error) error
	CommitOrRollback() error
	Rollback() error
	UnRegister(name string)
}

type Uow struct {
	DB           *sql.DB
	Tx           *sql.Tx
	Repositories map[string]RepositoryFactory
}

func NewUow(ctx context.Context, db *sql.DB) *Uow {
	return &Uow{
		DB: db,
		Repositories: make(map[string]RepositoryFactory),
	}
}

func (uow *Uow) Register(name string, factory RepositoryFactory) {
	uow.Repositories[name] = factory 
}

func (uow *Uow) UnRegister(name string) {
	delete(uow.Repositories,name) 
}

func (uow *Uow) Do(ctx context.Context, fn func(uow *Uow) error) error{
	//evitar iniciar uma nova transacao com o tx ja rodando uma transacao
	//verificar se o Tx do Uow esta ocupado
	if uow.Tx != nil {
		return fmt.Errorf("transaction already started")
	}

	//iniciar a transacao no db
	tx,err := uow.DB.BeginTx(ctx,nil)
	if err != nil {
		return err
	}
	uow.Tx = tx
	
	//executar a transacao com todos repositorios
	err = fn(uow)
	if err != nil {
		//rollback em caso de algum erro na transacao
		errRb := uow.Rollback()
		if errRb != nil {
			return errors.New(fmt.Sprintf("error: %s, error rollback: %s", err.Error(),errRb.Error()))
		}
		return err
	}

	return uow.CommitOrRollback()
}

func (uow *Uow) Rollback() error {
	//checar se existem transacoes rodando
	if uow.Tx == nil {
		return errors.New("no transactions to rollback")
	}
	err := uow.Tx.Rollback()
	if err != nil {
		return err
	}
	uow.Tx = nil
	return nil
}

func (uow *Uow) CommitOrRollback() error{
	//commit da transacao
	err := uow.Tx.Commit()
	if err != nil {
		//rollback em caso de algum erro na transacao
		errRb := uow.Rollback()
		if errRb != nil {
			return errors.New(fmt.Sprintf("error: %s, error rollback: %s", err.Error(),errRb.Error()))
		}
		return err
	}
	
	uow.Tx = nil
	return nil
}

func (uow *Uow) GetRepository(ctx context.Context, name string) (any, error) {
	//se nao haver tx cria o tx e inseri no uow
	if uow.Tx == nil {
		tx,err := uow.DB.BeginTx(ctx,nil)
		if err != nil {
			return nil,err
		}
		uow.Tx = tx
	}

	//pegar o repositorio com a transacao iniciada
	repository := uow.Repositories[name](uow.Tx)
	return repository, nil
}
