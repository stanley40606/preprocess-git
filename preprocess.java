import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class preprocess {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    String result = "";
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] ary = line.split(",",-1);
		float sum = 0;
		int turn = 0;
		float totalnum = 0;
		int i= 0;
		while(i<ary.length){
			if(i<=ary.length-26){
				while(turn<27){
				if(isInteger(ary[i])&& !(ary[i].equals(""))){
					sum += Float.parseFloat(ary[i]);
					System.out.print("");
					totalnum++;
				}
				i++;
				turn++;
				if(turn ==27){
					i=i-27;
				}
			}
			}
			
			if((ary[i].equals("") ||!(isInteger(ary[i])&& !(ary[i].equals(""))))&&(i%27!=0&&i%27!=1&&i%27!=2)){
	               if(totalnum==0){
	            	 ary[i]= "0";   
	               }else ary[i] = Float.toString(sum/totalnum);
	            }
	            if(i%26==0 && i!=0){
	               result = result + ary[i] + "\n";
	               turn = 0;
	               totalnum = 0;
	            }else result = result + ary[i] + ",";
	            i++;
		}
    }
    public void cleanup(Context context)throws IOException,InterruptedException{
       context.write(new Text(""),new Text(result));

    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for(Text data : values){
        context.write(key, new Text(data));
        } 
   }
 }
public static boolean isInteger(String str) {    
	    Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");    
	    return pattern.matcher(str).matches();    
	  }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);   
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
