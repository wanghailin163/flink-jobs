package com.iwellmass.demo.streaming


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic

/**
  * 滑动窗口计算
  *
  * 每隔1秒统计最近2秒内的数据，打印到控制台
  *
  */
object SocketWindowWordCountScala {


  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    //获取socket端口号
    val port: Int = try {
      params.getInt("port")
    }catch {
      case e: Exception => {
        System.err.println("No port set. use default port 9000--scala")
      }
        9002
    }

    //获取host
    val host: String = try {
      params.get("host")
    }catch {
      case e: Exception => {
        System.err.println("No host set. use default port 192.168.10.234--scala")
      }
        "192.168.10.234"
    }


    //获取环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置全局参数
    env.getConfig.setGlobalJobParameters(params)

    /*// 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)*/

    //链接socket获取输入数据
    val text = env.socketTextStream(host,port,'\n')

    //解析数据(把数据打平)，分组，窗口计算，并且聚合求sum
    val windowCounts = text.flatMap(line => line.split("\\s"))//打平，把每一行单词都切开
      .map(w => WordWithCount(w,1))//把单词转成word , 1这种形式
      .keyBy(wwc => wwc.word)
      //.keyBy("word")//分组
      .timeWindow(Time.seconds(2),Time.seconds(1))//指定窗口大小，指定间隔时间
      .sum("count");// sum或者reduce都可以
    //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    //打印到控制台
    windowCounts.print().setParallelism(1)

    //执行任务
    env.execute("Socket window count")


  }

  case class WordWithCount(word: String,count: Long)


}
