/*****************************
// Group - 3
// Group Members :
// Venkat Reddy Lingam - 700739897
// Sai Chaitanya Kammila - 700743027
******************************/

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class MatrixMulti {

    public static void main(String[] args)
            throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MatrixMulti <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(MatrixMulti.class);
        job.setJobName("Matrix Multi");

        job.getConfiguration().set("a", "7");
        job.getConfiguration().set("x", "8");
        job.getConfiguration().set("b", "10");
        //job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MatrixMultiMapper.class);
        job.setReducerClass(MatrixMultiReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class MatrixMultiMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int a = Integer.parseInt(conf.get("a"));
            int b = Integer.parseInt(conf.get("b"));

            String line = value.toString();
            String[] tokens = line.split(",");
            Text OutKey = new Text();
            Text OutValue = new Text();
            if (tokens[0].equals("A")) {
                for (int i = 0; i < b; i++) {
                    OutKey.set(tokens[1] + "," + i);
                    OutValue.set(tokens[0] + "," + tokens[2] + "," + tokens[3]);
                    context.write(OutKey, OutValue);
                }
            } else {
                for (int i = 0; i < a; i++) {
                    OutKey.set(i + "," + tokens[2]);
                    OutValue.set(tokens[0] + "," + tokens[1] + "," + tokens[3]);
                    context.write(OutKey, OutValue);
                }
            }
        }
    }

    public static class MatrixMultiReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int x = Integer.parseInt(conf.get("x"));

            int[] a = new int[x];
            int[] b = new int[x];
            for (Text value : values) {
                String[] token = value.toString().split(",");
                System.out.println(token);
                if (token[0].equals("A")) {
                    a[Integer.parseInt(token[1])] = Integer.parseInt(token[2]);
                } else {
                    b[Integer.parseInt(token[1])] = Integer.parseInt(token[2]);
                }

            }

            int sum = 0;
            for (int i = 0; i < x; i++) {
                sum = sum + a[i] * b[i];
            }
            Text outSum = new Text();
            outSum.set(String.valueOf(sum));
            context.write(key, outSum);

        }
    }
}
