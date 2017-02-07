import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class equijoin {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        //private final static IntWritable one = new IntWritable(1);
        private Text word1 = new Text();
        private Text word2 = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            if(tokens.length >= 2){
                word1.set(tokens[1]);
                word2.set(line);
                context.write(word1, word2);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            char curr='-';

            HashSet<String> hset1 = new HashSet<String>();
            for (Text val : values) {
                //System.out.print("Val is "+ val.toString());
                curr = val.toString().charAt(0);
                hset1.add(val.toString());
        }

        ArrayList<String> mapR = new ArrayList<String>();
        ArrayList<String> mapS = new ArrayList<String>();

            //if(hset1.size() > 1){
                Iterator<String> it = hset1.iterator();
                while (it.hasNext()) {
                    String temp = it.next();
                    //System.out.print("Inside the iterator "+ temp);
                    if(curr == temp.charAt(0)){
                        mapR.add(temp);
                    }else{
                        mapS.add(temp);
                    }
                }
                if (mapR.size() != 0 && mapS.size() != 0 ){
                    for(String str1: mapR){
                        for(String str2:mapS){
                            context.write(new Text(str1+","+str2),null);
                        }
                    }
                }
            //}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "equijoin");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		job.setJarByClass(equijoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
