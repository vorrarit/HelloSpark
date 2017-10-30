package com.pakgon.bigdata.test.hellospark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
		System.out.println( "Hello World!" );
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Hello Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile("hdfs:/user/maria_dev/movielens/movies.csv");
		textFile.foreach(new VoidFunction<String>(){
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
    }
}
