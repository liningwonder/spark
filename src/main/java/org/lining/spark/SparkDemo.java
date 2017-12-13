package org.lining.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * description:
 * date 2017/12/13
 *
 * @author lining1
 * @version 1.0.0
 */
public final class SparkDemo {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile("D:\\源码\\spark-2.2.1-bin-hadoop2.7\\README.md");
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //JavaRDD<String> word1 = input.flatMap(line -> Arrays.asList(SPACE.split(line)));

        //JavaRDD<String> words3 = input.flatMap(s -> Arrays.asList(SPACE.split(s)));

        //JavaRDD<String> count1 = input.filter(line -> line.contains("Python"));

        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        counts.saveAsTextFile("D:\\源码\\spark-2.2.1-bin-hadoop2.7\\out.text");
        sc.stop();
    }
}
