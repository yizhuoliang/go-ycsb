package paxos

import (
	"context"
	"errors"
	"sync/atomic"

	// "pc" for "paxos client"
	"github.com/magiconair/properties"
	"github.com/yizhuoliang/go-ycsb/pkg/ycsb"
	pc "github.com/yizhuoliang/gopaxos/paxosClient"
)

const (
	pcSerial       = "paxos.serial"
	pcSimon        = "paxos.simon"
	pcReplicaPort0 = "paxos.replica-port0"
	pcReplicaPort1 = "paxos.replica-port1"
)

type paxosDB struct {
	db *pc.Client
}

type paxosCreator struct {
	clientNum int32
}

func (c paxosCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	atomic.AddInt32(&c.clientNum, 1)
	// serial := p.GetInt(pcSerial, 0)
	simon := p.GetInt(pcSimon, 0)
	replicaPorts := make([]string, 2)
	replicaPorts[0] = p.GetString(pcReplicaPort0, "128.110.217.181:50051")
	replicaPorts[1] = p.GetString(pcReplicaPort1, "128.110.217.0:50052")
	// replicaPorts[0] = p.GetString(pcReplicaPort0, "localhost:50053")
	// replicaPorts[1] = p.GetString(pcReplicaPort1, "localhost:50054")
	client := pc.NewPaxosClient(int(c.clientNum), simon, replicaPorts)
	return &paxosDB{db: client}, nil
}

func (db *paxosDB) Close() error {
	return nil
}

func (db *paxosDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (db *paxosDB) CleanupThread(ctx context.Context) {
}

func (db *paxosDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Read(table + "%" + key)
	if err != nil {
		return nil, err
	}
	return map[string][]byte{fields[0]: []byte(value)}, nil
}

func (db *paxosDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	valueStr := ""
	for _, bytes := range values {
		valueStr += string(bytes)
	}
	return db.db.Store(table+"%"+key, valueStr)
}

func (db *paxosDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return db.Insert(ctx, table, key, values)
}

func (db *paxosDB) Delete(ctx context.Context, table string, key string) error {
	return errors.New("Sadly, we don't have this function!")
}

func (db *paxosDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, errors.New("Sadly, we don't have this function!")
}

func init() {
	ycsb.RegisterDBCreator("paxos", paxosCreator{})
}
