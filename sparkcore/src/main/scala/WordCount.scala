import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @author Lpy
  * @date 2021/3/1 16:04
  */
object WordCount {

  def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val sc = new SparkContext(conf)
    val sourceData = sc.textFile("datas")
    val mapData = sourceData.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    mapData.foreach(println)
    sc.stop()
  }

}
