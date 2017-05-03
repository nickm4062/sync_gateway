package base

import (
	"sync"
	"testing"

	"fmt"
	"log"
	"time"

	"encoding/json"
	"reflect"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/go.assert"
)

// func TransformBucketCredentials(inputUsername, inputPassword, inputBucketname string) (username, password, bucketname string) {

func TestTransformBucketCredentials(t *testing.T) {

	inputUsername := "foo"
	inputPassword := "bar"
	inputBucketName := "baz"

	username, password, bucketname := TransformBucketCredentials(
		inputUsername,
		inputPassword,
		inputBucketName,
	)
	assert.Equals(t, username, inputUsername)
	assert.Equals(t, password, inputPassword)
	assert.Equals(t, bucketname, inputBucketName)

	inputUsername2 := ""
	inputPassword2 := "bar"
	inputBucketName2 := "baz"

	username2, password2, bucketname2 := TransformBucketCredentials(
		inputUsername2,
		inputPassword2,
		inputBucketName2,
	)

	assert.Equals(t, username2, inputBucketName2)
	assert.Equals(t, password2, inputPassword2)
	assert.Equals(t, bucketname2, inputBucketName2)

}

// ----------------------------------------------------------------
var verbose = 2

type ExampleReceiver struct {
	m sync.Mutex

	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta map[uint16][]byte // To track metadata blob's per vbucketId.

	BucketName string
}

func (r *ExampleReceiver) OnError(err error) {
	if verbose > 0 {
		log.Printf("bucket: %s error: %v", r.BucketName, err)
		panic("Got error")
	}
}

func (r *ExampleReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if verbose > 1 {
		log.Printf("data-update: bucket: %s vbucketId: %d, key: %s, seq: %x, req: %#v",
			r.BucketName, vbucketId, key, seq, req)
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *ExampleReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if verbose > 1 {
		log.Printf("data-delete: bucket: %s vbucketId: %d, key: %s, seq: %x, req: %#v",
			r.BucketName, vbucketId, key, seq, req)
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *ExampleReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	if verbose > 1 {
		log.Printf("snapshot-start: bucket: %s vbucketId: %d, snapStart: %x, snapEnd: %x, snapType: %x",
			r.BucketName, vbucketId, snapStart, snapEnd, snapType)
	}
	return nil
}

func (r *ExampleReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	if verbose > 1 {
		log.Printf("set-metadata: bucket: %s vbucketId: %d, value: %s", r.BucketName, vbucketId, value)
	}

	r.m.Lock()
	defer r.m.Unlock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	return nil
}

func (r *ExampleReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	if verbose > 1 {
		log.Printf("get-metadata: bucket: %s vbucketId: %d", r.BucketName, vbucketId)
	}

	r.m.Lock()
	defer r.m.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

func (r *ExampleReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	if verbose > 0 {
		log.Printf("rollback: vbucketId: %d, rollbackSeq: %x", vbucketId, rollbackSeq)
	}

	return fmt.Errorf("unimpl-rollback")
}

// ----------------------------------------------------------------

func (r *ExampleReceiver) updateSeq(vbucketId uint16, seq uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
}

// ----------------------------------------------------------------

type authUserPswd struct{
	Username string
}

func (a authUserPswd) GetCredentials() (string, string, string) {
	return a.Username, "", ""
}


// Attempt to reproduce: https://github.com/couchbase/sync_gateway/issues/2514
// Error processing DCP stream: EOF -- base.(*DCPReceiver).OnError() at dcp_feed.go
func TestCBDatasourceConnectTwoBuckets(t *testing.T) {

	var serverURL = "http://192.168.33.10:8091"

	var poolName = "default"


	bucketNames := []string{
	"data-bucket-3",
	"data-bucket-4",
	}

	var bucketUUID = ""

	dcpFeedParams := cbgt.NewDCPFeedParams()
	options := &cbdatasource.BucketDataSourceOptions{
		ClusterManagerBackoffFactor: dcpFeedParams.ClusterManagerBackoffFactor,
		ClusterManagerSleepInitMS:   dcpFeedParams.ClusterManagerSleepInitMS,
		ClusterManagerSleepMaxMS:    dcpFeedParams.ClusterManagerSleepMaxMS,

		DataManagerBackoffFactor: dcpFeedParams.DataManagerBackoffFactor,
		DataManagerSleepInitMS:   dcpFeedParams.DataManagerSleepInitMS,
		DataManagerSleepMaxMS:    dcpFeedParams.DataManagerSleepMaxMS,

		FeedBufferSizeBytes:    dcpFeedParams.FeedBufferSizeBytes,
		FeedBufferAckThreshold: dcpFeedParams.FeedBufferAckThreshold,
	}

	serverURLs := []string{serverURL}
	bucketDataSources := []cbdatasource.BucketDataSource{}

	for _, bucketName := range bucketNames {
		var auth couchbase.AuthHandler = authUserPswd{
			Username: bucketName,
		}

		receiver := &ExampleReceiver{
			BucketName: bucketName,
		}

		vbucketIdsArr := []uint16(nil) // A nil means get all the vbuckets.

		bds, err := cbdatasource.NewBucketDataSource(serverURLs,
			poolName, bucketName, bucketUUID, vbucketIdsArr, auth, receiver, options)
		if err != nil {
			log.Fatalf(fmt.Sprintf("error: NewBucketDataSource, err: %v", err))
		}

		if err = bds.Start(); err != nil {
			log.Fatalf(fmt.Sprintf("error: Start, err: %v", err))
		}

		if verbose > 0 {
			log.Printf("started bucket data source: %v", bds)
		}

		bucketDataSources = append(bucketDataSources, bds)
	}



	for {
		time.Sleep(1000 * time.Millisecond)
		// log.Printf("bds0 stats -----------------------------")
		// reportStats(bucketDataSources[0], true)
		// log.Printf("bds1 stats -----------------------------")
		// reportStats(bucketDataSources[1], true)

	}

}

// ----------------------------------------------------------------

var mutexStats sync.Mutex
var lastStats = &cbdatasource.BucketDataSourceStats{}
var currStats = &cbdatasource.BucketDataSourceStats{}

func reportStats(b cbdatasource.BucketDataSource, force bool) {
	if verbose <= 0 {
		return
	}

	mutexStats.Lock()
	defer mutexStats.Unlock()

	b.Stats(currStats)
	if force || !reflect.DeepEqual(lastStats, currStats) {
		buf, err := json.Marshal(currStats)
		if err == nil {
			log.Printf("%s", string(buf))
		}
		lastStats, currStats = currStats, lastStats
	}
}
