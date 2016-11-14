package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/xxoommd/leaf/chanrpc"
)

// NewProcessor creates a Processor for leaf communication
func NewProcessor() *Proccessor {
	p := new(Proccessor)
	p.msgIDMap = make(map[reflect.Type]uint16)
	return p
}

// Proccessor struct
type Proccessor struct {
	msgInfoArr [math.MaxUint16 + 1]*MsgInfo
	msgIDMap   map[reflect.Type]uint16
}

// MsgInfo struct
type MsgInfo struct {
	msgType   reflect.Type
	msgRouter *chanrpc.Server
}

// Register method
// 'msg' should have 'MarshalBinary' and 'UnmarshalBinary' method
func (p *Proccessor) Register(msgID uint16, msg interface{}, chanRPC *chanrpc.Server) {
	errPrefix := "[processor] Register failed."
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		panic(errPrefix + "protobuf message pointer required")
	}

	// Check methods
	if _, ok := msgType.MethodByName("MarshalBinary"); !ok {
		panic(fmt.Sprintf("%s %s has no method 'MarshalBinary'", errPrefix, msgType))
	}
	if _, ok := msgType.MethodByName("UnmarshalBinary"); !ok {
		panic(fmt.Sprintf("%s %s has no method 'UnmarshalBinary'", errPrefix, msgType))
	}

	if _, ok := p.msgIDMap[msgType]; ok {
		panic(fmt.Sprintf("%s msg: '%s' is already registered", errPrefix, msgType))
	}

	if chanRPC == nil {
		panic(errPrefix + " Invalid chanrpc")
	}

	i := &MsgInfo{
		msgRouter: chanRPC,
		msgType:   msgType,
	}
	p.msgInfoArr[msgID] = i
	p.msgIDMap[msgType] = msgID
}

// Route method: goroutine safe
func (p *Proccessor) Route(msg interface{}, userData interface{}) error {
	errPrefix := "[processor] Route error."
	msgType := reflect.TypeOf(msg)
	msgID, ok := p.msgIDMap[msgType]
	if !ok {
		return fmt.Errorf("%s msg %s not registered", errPrefix, msgType)
	}

	info := p.msgInfoArr[msgID]
	if info == nil {
		return fmt.Errorf("%s msg %s not registered", errPrefix, msgType)
	}

	if info.msgRouter == nil {
		return fmt.Errorf("%s Invalid chanrpc", errPrefix)
	}

	return info.msgRouter.Call0(msgType, msg, userData)
}

// Unmarshal method: goroutine safe
// 'msg' should have method 'UnmarshalBinary' method which need a []byte parameter
func (p *Proccessor) Unmarshal(data []byte) (interface{}, error) {
	errorPrefix := "[processor] Unmarshal error:"
	if len(data) < 2 {
		return nil, fmt.Errorf("%s data length < 2", errorPrefix)
	}

	msgID := binary.BigEndian.Uint16(data[:2])
	info := p.msgInfoArr[msgID]
	if info == nil {
		return nil, fmt.Errorf("%s invalid msgID:%d", errorPrefix, msgID)
	}

	msgType := info.msgType
	msg := reflect.New(msgType.Elem())
	_, ok := msg.Type().MethodByName("UnmarshalBinary")
	if !ok {
		return nil, fmt.Errorf("%s should have 'UnmarshalBinary' method", msgType)
	}

	arg := reflect.ValueOf(data[2:])
	// TODO how to handle errors in func itself?
	msg.MethodByName("UnmarshalBinary").Call([]reflect.Value{arg})

	return msg.Interface(), nil
}

// Marshal method: goroutine safe
// 'msg' must have method 'MarshalBinary' which return []byte
func (p *Proccessor) Marshal(msg interface{}) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	msgID, ok := p.msgIDMap[msgType]
	if !ok {
		return nil, fmt.Errorf("[processor] Marshal error. %s msgID not found", msgType)
	}

	if _, ok := msgType.MethodByName("MarshalBinary"); !ok {
		return nil, fmt.Errorf("[processor] Marshal error. %s should have 'MarshalBinary' method", msgType)
	}

	v := reflect.ValueOf(msg)
	result := v.MethodByName("MarshalBinary").Call([]reflect.Value{})
	if len(result) == 0 {
		return nil, fmt.Errorf("[processor] Marshal error. %s.MarshalBinary failed", msgType)
	}

	idBin := make([]byte, 2)
	binary.BigEndian.PutUint16(idBin, msgID)
	msgBin := result[0].Interface().([]byte)
	return [][]byte{idBin, msgBin}, nil
}
