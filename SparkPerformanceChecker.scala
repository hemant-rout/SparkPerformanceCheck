package com.data.spark

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.parsing.json._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SparkPerformanceChecker {

    /*
    * This is function is used to get performance matrices for spark job
    * @sparkSession This is the SparkSession of current Spark job
     *@eventLogBasePath This argument is the base path for log events
     *@destinationPath This argument used to output the spark job performance matrices
    * */
    def getSparkPerformanceStatistics(sparkSession:SparkSession)(eventLogBasePath:String)(destinationPath:String): Unit = {
      val appId:String=sparkSession.sparkContext.applicationId
      sparkSession.stop()
      val eventLogPath: String = eventLogBasePath+"/"+ appId
      var resultProperties: ArrayBuffer[Any] = ArrayBuffer[Any]()
      try {
        val source:Source = Source.fromFile(eventLogPath)
          val sourceRecords=source.getLines()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        sourceRecords.foreach(line => {
          val jsonObjects = JSON.parseFull(line)
          jsonObjects.foreach(x => {
            val properties = x.asInstanceOf[Map[String, Any]]
            val filterCriteria: String = properties.get("Event").get.asInstanceOf[String]
            if (filterCriteria == "SparkListenerApplicationStart") {
              resultProperties += properties.get("App ID").get.asInstanceOf[String]
              resultProperties += properties.get("App Name").get.asInstanceOf[String]
              val timeInLong = properties.get("Timestamp").get.asInstanceOf[Double].toLong
              resultProperties += sdf.format(timeInLong)
            } else if (filterCriteria == "SparkListenerApplicationEnd") {
              val timeInLong = properties.get("Timestamp").get.asInstanceOf[Double].toLong
              resultProperties += sdf.format(timeInLong)
            }
          })
        })
        source.close()
        FileUtils.forceDeleteOnExit(new File(eventLogPath))
      }catch{
        case io:IOException=> throw new RuntimeException(s"Exception has been occured, error being :Unable to process file :$eventLogPath "+io.getCause)
        case e:Exception=> throw new RuntimeException(s"Exception has been occurred, error being :"+e.getCause)
      }
      try {
        val spark = SparkSession.builder().appName("SparkDataFrameExample").config("spark.tmp.warehouse", "file:///tmp").master("local").getOrCreate()
        val schema: StructType = StructType(Seq(StructField("AppId", StringType), StructField("AppName", StringType), StructField("StartTime", StringType), StructField("EndTime", StringType)))
        val rdd: RDD[Row] = spark.sparkContext.makeRDD(List(Row.fromSeq(resultProperties)))
        val dataFrame: DataFrame = spark.createDataFrame(rdd, schema)
        dataFrame.write.mode("append").json(destinationPath)
      }catch{
        case e:Exception=> throw new RuntimeException(s"Exception has been occurred, error being :"+e.getCause)
      }
    }

}
