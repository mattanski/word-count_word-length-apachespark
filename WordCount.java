package example.com;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.List;



public class WordCount {


        public static void main(String[] args) {


            JavaSparkContext sparkContext = new JavaSparkContext("local[*]","SPARKexample");

            JavaRDD<String> inputFile = sparkContext.textFile("C:\\Users\\U-19\\Desktop\\FILE DI INSTALLAZIONE\\input.txt");

            JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

            JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

            countData.saveAsTextFile("C:\\Users\\U-19\\Desktop\\FILE DI INSTALLAZIONE\\output.txt");

           
            
            JavaRDD<String> wordsFromFiles = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());

            List<String> words = wordsFromFiles.collect();
            for (String word : words) {
               int wordLength = word.length();
                System.out.println("PAROLA: " + word + "                                   LUNGHEZZA DELLA PAROLA: " + wordLength );

            }
            
            System.out.println("\n\n\n\n\n\n\n");
            
            double lunghezzaMedia = inputFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToDouble(String::length).mean();
            System.out.println("LA LUNGHEZZA MEDIA DELLE PAROLE: " + lunghezzaMedia);
            
            
            System.out.println("\n\n\n\n\n\n\n");
            
            
            JavaRDD<String> longWords = wordsFromFile.filter(word -> word.length() > 5);

            longWords.foreach(word -> {
                int wordLength = word.length();
                System.out.println("PAROLA: " + word + ", LUNGHEZZA DELLA PAROLA: " + wordLength);
            });
                
                    
                    
                    
            sparkContext.close();
                    
                    
            }
            
            
            
        }
      

//List<String> wordx = wordsFromFiles.collect();
//for (String x : wordx) {
//    int wordLengthx = x.length();
//    if (wordLengthx > 5) {
//    System.out.println("PAROLA: " + x + "                                   LUNGHEZZA DELLA PAROLA: " + wordLengthx);
//    }
