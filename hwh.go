package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/rest"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	rest.RestConf
	FilePath struct {
		ArchivePath string
		DataPath    string
	}
}

type ArchiveParam struct {
	ArchiveTimeSeries string `bson:"archive_time_series"`
	ArchiveDate       string `bson:"archive_date"`
	ArchiveTime       string `bson:"archive_time"`
	GroupName         string `bson:"group_name"`
	ModuleName        string `bson:"module_name"`
	WorkpieceNum      string `bson:"workpiece_num"`
	StationNum        string `bson:"station_num"`
	ProcessNum        string `bson:"process_num"`
	CarType           string `bson:"car_type"`
	Mode              string `bson:"mode"`
	TypeId            string `bson:"type_id"`
	SpotNum           string `bson:"spot_num"`
	ProgramNo         string `bson:"program_no"`
	SpotName          string `bson:"spot_name"`
	BasicCurrent      string `bson:"basic_current"`
	PreTime           string `bson:"pre_time"`
	WeldTime          string `bson:"weld_time"`
	KeepTime          string `bson:"keep_time"`
	Msg               string `bson:"msg"`
	GunNum            string `bson:"gun_num"`
	GunName           string `bson:"gun_name"`
	QActive           string `bson:"q_active"`
	QSpotSet          string `bson:"q_spot_set"`
	QSpotValue        string `bson:"q_spot_value"`
	QSpotState        string `bson:"q_spot_state"`
	SpatterRate       string `bson:"spatter_rate"`
	SKT               string `bson:"skt"`
	IKV               string `bson:"ikv"`
	UKV               string `bson:"ukv"`
	RKV               string `bson:"rkv"`
	QRkv              string `bson:"q_rkv"`
}

var configFile = flag.String("f", "hwh-config.yaml", "the config file")

func confParse() *Config {
	var data Config
	flag.Parse()
	conf.MustLoad(*configFile, &data)
	return &data
}

func main() {
	archivePath := confParse().FilePath.ArchivePath
	dataPath := confParse().FilePath.DataPath
	Start(archivePath, dataPath)

}

func Start(archivePath string, dataPath string) {
	filenameWithSuffix := path.Base(archivePath)
	fileSuffix := path.Ext(filenameWithSuffix)
	if fileSuffix == ".xarch" {
		dataA(archivePath, dataPath)
	} else if fileSuffix == ".xcarch" {
		dataB(archivePath, dataPath)
	}

}

func dataA(archivePath string, dataPath string) {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // true:一直监听(同tail -f) false:一次后即结束
		Location:  &tail.SeekInfo{Offset: 0, Whence: 1}, // 从文件的哪个地方开始读
		MustExist: false,                                // true:文件不存在即退出 false:文件不存在即等待
		Poll:      true,
	}
	tails, err := tail.TailFile(archivePath, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
		return
	}

	var (
		line *tail.Line
		ok   bool
	)

	for {
		line, ok = <-tails.Lines
		if !ok {
			fmt.Printf("tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
			continue
		}
		str1 := strings.Replace(line.Text, "/", " ", -1)
		str2 := strings.Replace(str1, " ", "0", -1)
		str3 := strings.Replace(str2, "|", " ", -1)
		items := strings.Fields(str3)

		if len(items) > 20 {
			data := ArchiveDataA(items).(ArchiveParam)

			skt, _ := json.Marshal(timerCurveParsing(data.SKT))
			ikv, _ := json.Marshal(timerCurveParsing(data.IKV))
			ukv, _ := json.Marshal(timerCurveParsing(data.UKV))
			rkv, _ := json.Marshal(timerCurveParsing(data.RKV))
			qRkv, _ := json.Marshal(timerCurveParsing(data.QRkv))

			sktFilename := dataPath + data.GroupName + "_" + data.ModuleName + "_" + data.ArchiveTimeSeries + "_skt" + ".json"
			ikvFilename := dataPath + data.GroupName + "_" + data.ModuleName + "_" + data.ArchiveTimeSeries + "_current" + ".json"
			ukvFilename := dataPath + data.GroupName + "_" + data.ModuleName + "_" + data.ArchiveTimeSeries + "_voltage" + ".json"
			rkvFilename := dataPath + data.GroupName + "_" + data.ModuleName + "_" + data.ArchiveTimeSeries + "_resistance" + ".json"
			qRkvFilename := dataPath + data.GroupName + "_" + data.ModuleName + "_" + data.ArchiveTimeSeries + "_Q-resistance" + ".json"

			sktFile, _ := os.Create(sktFilename)
			defer sktFile.Close()
			_, err = sktFile.Write(skt)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + sktFilename + " create success")

			ikvFile, _ := os.Create(ikvFilename)
			defer ikvFile.Close()
			_, err = ikvFile.Write(ikv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + ikvFilename + " create success")

			ukvFile, _ := os.Create(ukvFilename)
			defer ukvFile.Close()
			_, err = ukvFile.Write(ukv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + ukvFilename + " create success")

			rkvFile, _ := os.Create(rkvFilename)
			defer rkvFile.Close()
			_, err = rkvFile.Write(rkv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + rkvFilename + " create success")

			qRkvFile, _ := os.Create(qRkvFilename)
			defer qRkvFile.Close()
			_, err = qRkvFile.Write(qRkv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + qRkvFilename + " create success")

		}

	}
}

func dataB(archivePath string, dataPath string) {
	config := tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // true:一直监听(同tail -f) false:一次后即结束
		Location:  &tail.SeekInfo{Offset: 0, Whence: 1}, // 从文件的哪个地方开始读
		MustExist: false,                                // true:文件不存在即退出 false:文件不存在即等待
		Poll:      true,
	}
	tails, err := tail.TailFile(archivePath, config)
	if err != nil {
		fmt.Println("tail file failed, err:", err)
		return
	}

	var (
		line *tail.Line
		ok   bool
	)

	for {
		line, ok = <-tails.Lines
		if !ok {
			fmt.Printf("tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
			continue
		}
		str1 := strings.Replace(line.Text, "/", " ", -1)
		str2 := strings.Replace(str1, "|", " ", -1)
		items := strings.Fields(str2)

		if len(items) > 20 {
			data := ArchiveDataB(items).(ArchiveParam)

			skt, _ := json.Marshal(timerCurveParsing(data.SKT))
			ikv, _ := json.Marshal(timerCurveParsing(data.IKV))
			ukv, _ := json.Marshal(timerCurveParsing(data.UKV))
			rkv, _ := json.Marshal(timerCurveParsing(data.RKV))
			qRkv, _ := json.Marshal(timerCurveParsing(data.QRkv))
			d, _ := json.Marshal(timerCurveParsing(data.SKT))

			dataFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_data" + ".json"
			sktFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_skt" + ".json"
			ikvFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_current" + ".json"
			ukvFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_voltage" + ".json"
			rkvFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_resistance" + ".json"
			qRkvFilename := dataPath + data.ModuleName + "_" + data.ArchiveTimeSeries + "_Q-resistance" + ".json"

			dataFile, _ := os.Create(dataFilename)
			defer dataFile.Close()
			_, err = dataFile.Write(d)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + sktFilename + " create success")

			sktFile, _ := os.Create(sktFilename)
			defer sktFile.Close()
			_, err = sktFile.Write(skt)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + sktFilename + " create success")

			ikvFile, _ := os.Create(ikvFilename)
			defer ikvFile.Close()
			_, err = ikvFile.Write(ikv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + ikvFilename + " create success")

			ukvFile, _ := os.Create(ukvFilename)
			defer ukvFile.Close()
			_, err = ukvFile.Write(ukv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + ukvFilename + " create success")

			rkvFile, _ := os.Create(rkvFilename)
			defer rkvFile.Close()
			_, err = rkvFile.Write(rkv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + rkvFilename + " create success")

			qRkvFile, _ := os.Create(qRkvFilename)
			defer qRkvFile.Close()
			_, err = qRkvFile.Write(qRkv)
			if err != nil {
				panic(err)
			}
			fmt.Println("File :" + qRkvFilename + " create success")
		}

	}

}

func ArchiveDataA(slice []string) interface{} {
	items := ArchiveParam{}
	items.GroupName = "0"
	items.ModuleName = slice[1]
	items.ArchiveTime = slice[3]
	items.ModuleName = slice[0]
	items.ArchiveTime = slice[7]
	items.ArchiveDate = slice[6]
	items.ArchiveTimeSeries = slice[78]
	items.StationNum = "0"
	items.ProcessNum = "0"
	items.CarType = "0"

	if i := Find(slice, "14119"); i != -1 {
		items.WorkpieceNum = "0"
	} else {
		items.WorkpieceNum = "0"
	}

	if i := Find(slice, "14049"); i != -1 {
		switch slice[i-15] {
		case "1":
			items.Mode = "SKT"
		case "2":
			items.Mode = "KSR"
		case "4":
			items.Mode = "IQR"
		case "5":
			items.Mode = "AMC/DCM"
		case "9":
			items.Mode = "AMF"
		default:
			items.Mode = "0"
		}
	} else {
		items.Mode = "0"
	}
	if i := Find(slice, "14408"); i != -1 {
		items.SpotNum = slice[i-15]
	} else {
		items.SpotNum = "0"
	}
	if i := Find(slice, "14409"); i != -1 {
		items.TypeId = slice[i-15]
	} else {
		items.TypeId = "0"
	}

	if i := Find(slice, "3528"); i != -1 {
		items.ProgramNo = "0"
	} else {
		items.ProgramNo = "0"
	}
	if i := Find(slice, "14067"); i != -1 {
		items.SpotName = "0"
	} else {
		items.SpotName = "0"
	}
	if i := Find(slice, "8077"); i != -1 {
		items.BasicCurrent = slice[i-15]
	} else {
		items.BasicCurrent = "0"
	}
	if i := Find(slice, "151"); i != -1 {
		items.PreTime = "0"
	} else {
		items.PreTime = "0"
	}
	if i := Find(slice, "14219"); i != -1 {
		items.WeldTime = slice[i-15]
	} else {
		items.WeldTime = "0"
	}
	if i := Find(slice, "152"); i != -1 {
		items.KeepTime = "0"
	} else {
		items.KeepTime = "0"
	}
	if i := Find(slice, "14512"); i != -1 {
		items.Msg = "0"
	} else {
		items.Msg = "0"
	}
	if i := Find(slice, "3529"); i != -1 {
		items.GunNum = "0"
	} else {
		items.GunNum = "0"
	}
	if i := Find(slice, "625"); i != -1 {
		items.GunName = slice[i-3]
	} else {
		items.GunName = "0"
	}
	if i := Find(slice, "14280"); i != -1 {
		items.QActive = slice[i-15]
	} else {
		items.QActive = "0"
	}
	if i := Find(slice, "14285"); i != -1 {
		items.QSpotSet = slice[i-15]
	} else {
		items.QSpotSet = "0"
	}
	if i := Find(slice, "14287"); i != -1 {
		items.QSpotValue = slice[i-15]
	} else {
		items.QSpotValue = "0"
	}
	if items.QSpotValue >= items.QSpotSet {
		items.QSpotState = "OK"
	} else {
		items.QSpotState = "NOK"
	}

	if i := Find(slice, "14455"); i != -1 {
		items.SpatterRate = "0"
	} else {
		items.SpatterRate = "0"
	}

	if i := Find(slice, "14095"); i != -1 {
		items.SKT = slice[i-7]
	} else {
		items.SKT = "0"
	}
	if i := Find(slice, "14055"); i != -1 {
		items.IKV = slice[i-7]
	} else {
		items.IKV = "0"
	}
	if i := Find(slice, "14057"); i != -1 {
		items.UKV = slice[i-7]
	} else {
		items.UKV = "0"
	}
	if i := Find(slice, "14740"); i != -1 {
		items.RKV = slice[i-7]
	} else {
		items.RKV = "0"
	}
	if i := Find(slice, "14281"); i != -1 {
		items.QRkv = slice[i-7]
	} else {
		items.QRkv = "0"
	}

	return items
}

func ArchiveDataB(slice []string) interface{} {
	items := ArchiveParam{}
	items.GroupName = slice[0]
	items.ModuleName = slice[1]
	items.ArchiveTime = slice[3]
	items.ArchiveDate = slice[4]
	items.ArchiveTimeSeries = slice[5]
	items.StationNum = "0"
	items.ProcessNum = "0"
	items.CarType = "0"

	if i := Find(slice, "14119"); i != -1 {
		items.WorkpieceNum = slice[i+1]
	} else {
		items.WorkpieceNum = "0"
	}

	if i := Find(slice, "14049"); i != -1 {
		switch slice[i+1] {
		case "1":
			items.Mode = "SKT"
		case "2":
			items.Mode = "KSR"
		case "4":
			items.Mode = "IQR"
		case "5":
			items.Mode = "AMC/DCM"
		case "9":
			items.Mode = "AMF"
		default:
			items.Mode = "0"
		}
	} else {
		items.Mode = "0"
	}
	if i := Find(slice, "14409"); i != -1 {
		items.TypeId = slice[i+1]
	} else {
		items.TypeId = "0"
	}
	if i := Find(slice, "14408"); i != -1 {
		items.SpotNum = slice[i+1]
	} else {
		items.SpotNum = "0"
	}
	if i := Find(slice, "3528"); i != -1 {
		items.ProgramNo = slice[i+1]
	} else {
		items.ProgramNo = "0"
	}
	if i := Find(slice, "14067"); i != -1 {
		items.SpotName = slice[i+1]
	} else {
		items.SpotName = "0"
	}
	if i := Find(slice, "8077"); i != -1 {
		items.BasicCurrent = slice[i+1]
	} else {
		items.BasicCurrent = "0"
	}
	if i := Find(slice, "151"); i != -1 {
		items.PreTime = slice[i+1]
	} else {
		items.PreTime = "0"
	}
	if i := Find(slice, "14219"); i != -1 {
		items.WeldTime = slice[i+1]
	} else {
		items.WeldTime = "0"
	}
	if i := Find(slice, "152"); i != -1 {
		items.KeepTime = slice[i+1]
	} else {
		items.KeepTime = "0"
	}
	if i := Find(slice, "14512"); i != -1 {
		items.Msg = slice[i+1]
	} else {
		items.Msg = "0"
	}
	if i := Find(slice, "3529"); i != -1 {
		items.GunNum = slice[i+1]
	} else {
		items.GunNum = "0"
	}
	if i := Find(slice, "625"); i != -1 {
		items.GunName = slice[i+1]
	} else {
		items.GunName = "0"
	}
	if i := Find(slice, "14280"); i != -1 {
		items.QActive = slice[i+1]
	} else {
		items.QActive = "0"
	}
	if i := Find(slice, "14285"); i != -1 {
		items.QSpotSet = slice[i+1]
	} else {
		items.QSpotSet = "0"
	}
	if i := Find(slice, "14287"); i != -1 {
		items.QSpotValue = slice[i+1]
	} else {
		items.QSpotValue = "0"
	}
	if items.QSpotValue >= items.QSpotSet {
		items.QSpotState = "OK"
	} else {
		items.QSpotState = "NOK"
	}

	if i := Find(slice, "14455"); i != -1 {
		items.SpatterRate = slice[i+1]
	} else {
		items.SpatterRate = "0"
	}

	if i := Find(slice, "14095"); i != -1 {
		items.SKT = slice[i+1]
	} else {
		items.SKT = "0"
	}
	if i := Find(slice, "14055"); i != -1 {
		items.IKV = slice[i+1]
	} else {
		items.IKV = "0"
	}
	if i := Find(slice, "14057"); i != -1 {
		items.UKV = slice[i+1]
	} else {
		items.UKV = "0"
	}
	if i := Find(slice, "14740"); i != -1 {
		items.RKV = slice[i+1]
	} else {
		items.RKV = "0"
	}
	if i := Find(slice, "14281"); i != -1 {
		items.QRkv = slice[i+1]
	} else {
		items.QRkv = "0"
	}

	return items
}

func Find(slice []string, val string) int {
	for i, item := range slice {
		if item == val {
			return i
		}
	}
	return -1
}

func timerCurveParsing(data string) interface{} {
	var i uint64
	var measuredValue float32
	var result []float32
	if len(data) > 50 && data != "null" && data != "0" && data != "000" {
		factor, _ := strconv.ParseUint(string(data[30])+string(data[31])+string(data[28])+string(data[29])+
			string(data[26])+string(data[27])+string(data[24])+string(data[25]), 16, 0)
		sampleNum, _ := strconv.ParseUint(string(data[34])+string(data[35])+string(data[32])+string(data[33]),
			16, 0)
		for range data {
			if i+1 < sampleNum {
				value, _ := strconv.ParseUint(string(data[38+i*4])+string(data[39+i*4])+string(data[36+i*4])+
					string(data[37+i*4]), 16, 0)
				measuredValue = (float32(value) * float32(factor)) / float32(65536)
				result = append(result, measuredValue)
			}
			i++
		}
	} else {
		result = append(result, 0)
	}

	return result
}
