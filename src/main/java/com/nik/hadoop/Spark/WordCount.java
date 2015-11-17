package com.nik.hadoop.Spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

// http://www.robertomarchetto.com/spark_java_maven_example
// hdfs://localhost:54310/user/hduser/data/googlebooks-eng-all-1gram-20090715-0.tsv

/**
 * hadoop fs -mkdir spark_data spark_output
 * hadoop fs -put loremipsum.txt spark_data/loremipsum.txt
 * 
 * $HOME/Downloads/Apache/spark-1.5.1-bin-hadoop2.6/bin/spark-submit \
	--class com.nik.hadoop.Spark.WordCount \
	--master local[2] Spark-0.0.1-SNAPSHOT.jar
 *  
 * */
public class WordCount {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("com.nik.hadoop.Spark.WordCount").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> textFile = context.textFile("hdfs://localhost:54310/spark_data/loremipsum.txt");
    JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
    });
    
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
    });
    
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer a, Integer b) { return a + b; }
    });
    
    counts.saveAsTextFile("hdfs://localhost:54310/user/hduser/spark_output"); 
    
    if(context!=null ) {
    	context.close();
    }
  }
}