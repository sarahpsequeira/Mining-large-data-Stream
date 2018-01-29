package miningdatastream;
/**
 *
 * @author sarah
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;


public class MiningDataStream {

   //create a lock
   static Semaphore lock = new Semaphore(1);
   // create a list to store the input stream 
   static List<Integer> Input_Stream  = new ArrayList<Integer>();
   //Create Bucket
   static List<List<Integer>> Bucket = new ArrayList<List<Integer>>();
   static int window_size=0;
   static int timer_count=-1;
   static String host="";
   static int port=0;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        String input_host_port_pair="";
        String SplitColon = ":";
        String input="";
        BufferedReader br= new BufferedReader(new InputStreamReader(System.in)) ;
        
        while(true)
        {
            input=br.readLine();
            if(input.startsWith("#"))
            {
                continue;
            }
            else if (input.contains("#"))
            {
                input=input.substring(0,input.indexOf("#"));
                break;
            }
            else
            {
                break;
            }
        }
        //Read the window size
        try
        {
        MiningDataStream.window_size=Integer.parseInt(input.trim());
        }catch(NumberFormatException e)
        {
            System.out.print("Invalid window size");
            System.exit(0);
        }
        if(MiningDataStream.window_size<=0)
        {
            System.out.println("Invalid Input");
            System.exit(0);
        }
             
        //Connection
        while(true)
        {
            input_host_port_pair=br.readLine();
            if(input_host_port_pair.startsWith("#"))
            {
                continue;
            }
            else if (input_host_port_pair.contains("#"))
            {
                input_host_port_pair=input_host_port_pair.substring(0,input_host_port_pair.indexOf("#"));
                break;
            }
            else
            {
                break;
            }
        }
        
        String [] host_port =input_host_port_pair.split(SplitColon);
        host=host_port[0];
        try{
        port=Integer.parseInt(host_port[1]);
        }
        catch(NumberFormatException e)
        {
            System.out.println("Invalid port");
            System.exit(0);
        }
        //Check port validity
        if(port<1023 || port>65535)
        {
            System.out.println("Invalid port");
            System.exit(0);
        }
        //Check for IP validity
        if(!(host.matches(".*[a-z].*")))
        {
            if(host.split("\\.").length!=4)
            {
                System.out.println("Invalid IP Address");
                System.exit(0);
            }
        }
        try{
            InetAddress  IP_Address = InetAddress.getByName(host);
            host=IP_Address.toString().split("/")[1];
        }
        catch(Exception e)
        {
            System.out.println("Invalid Address");
            System.exit(0);
        }
        
        new Thread(new Thread1()).start();
        new Thread(new Thread2()).start();
    }
}

class Thread1 implements Runnable{
    public void run(){
        
        try {            
            
            Socket s = new Socket();
            try {
            s = new Socket(MiningDataStream.host,MiningDataStream.port);
            }
            catch(Exception e)
            {
                System.out.println("Server not available");
                System.exit(0);
            }
            //Socket s = new Socket("127.0.0.1",3333);
            
            //Get Input
            BufferedReader din=new BufferedReader(new InputStreamReader(s.getInputStream()));
            String str="";
            
            // Declarations
            int input_bit;
            int k=0;
            int first_value=0;
            int second_value=0;
            
            //Parsing the input
            while((str=din.readLine()) != null)
            {
                MiningDataStream.lock.acquire();
                input_bit=Integer.parseInt(str);
                if(MiningDataStream.Input_Stream.size()==MiningDataStream.window_size)
                {
                    MiningDataStream.Input_Stream.remove(0);
                }
                MiningDataStream.Input_Stream.add(input_bit);
                MiningDataStream.timer_count=MiningDataStream.timer_count+1;
                //AcquirMiningDataStreaming lock to print the input stream
                
                System.out.print(input_bit);
             
                //Adding to bucket
                if(input_bit==1)
                {
                    List <Integer> temp = new ArrayList<Integer>();
                    temp.add(MiningDataStream.timer_count);
                    temp.add(1);
                    MiningDataStream.Bucket.add(temp);
                    
                    //Merging Buckets
                    k=MiningDataStream.Bucket.size()-1;
                    while(k-3>=0)
                    {
                        if(MiningDataStream.Bucket.get(k-3).get(1)==MiningDataStream.Bucket.get(k).get(1))
                        {
                            first_value=MiningDataStream.Bucket.get(k-3).get(1);
                            second_value=MiningDataStream.Bucket.get(k-2).get(1);
                            MiningDataStream.Bucket.get(k-2).set(1,first_value+second_value);
                            MiningDataStream.Bucket.remove(k-3);
                            k=k-3;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                MiningDataStream.lock.release();                           
            }
            //System.out.println(Input_Stream);
            //System.out.println(Bucket);
            
        //s.close();
        } catch (Exception e) {
            e.printStackTrace();
           // Logger.getLogger(Run.class.getName()).log(Level.SEVERE, null, ex);
        }      
    }
}

class Thread2 implements Runnable{

    @Override
    public void run() {
        //System.out.println("Thread2");
        long query_bit=0;
        boolean increment=false;
        String test_string="w";
        int ch;
        String Valid_Query[]="What is the number of ones for last K data?".toLowerCase().split(" ");
        
        while(true){
        BufferedReader bd= new BufferedReader(new InputStreamReader(System.in)) ;
        try {
            if(!((ch=bd.read())=='\0'))
            {
                //Acquiring the lock
                MiningDataStream.lock.acquire();
                //Read the query
                String Query=bd.readLine();
                if(ch==87 || ch ==119)
                {
                    Query=test_string+Query;
                }
                //Print the query
                //System.out.println(Query);
                
                //Parse the query
                String Parsed_Query[]=Query.toLowerCase().split(" ");
                boolean valid=true;
                if(Parsed_Query.length==Valid_Query.length)
                {
                    for(int i=0;i<Parsed_Query.length;i++)
                    {
                        if(i!=8)
                        {
                            if(!(Parsed_Query[i].equals(Valid_Query[i])))
                            {
                                valid=false;
                                break;
                            }
                        }
                        else
                        {
                            try
                            {
                             long temp=Long.parseLong(Parsed_Query[i]);
                            }
                            catch(NumberFormatException e){
                                valid=false;
                                break;
                            }
                        }
                    }
                }
                else
                {
                    valid=false;
                }
                if(valid==false)
                {
                    //valid=true;
                    System.out.println("Invalid Query");
                    MiningDataStream.lock.release();
                    continue;
                }
                //Check if the query is valid
                query_bit=Long.parseLong(Parsed_Query[8]);
                
                int count_ones=0;
                increment=false;
                //String Query=bd.readLine();
                //check if query is in right format What is the number of ones for last <k> data?
                if(query_bit<=MiningDataStream.window_size)
                {
                    //Calculate count from window
                    if (MiningDataStream.Input_Stream.size()>=query_bit)
                    {
                        for (int i=MiningDataStream.Input_Stream.size()-1;i>=MiningDataStream.Input_Stream.size()-query_bit;i--)
                        {   
                            if(MiningDataStream.Input_Stream.get(i).equals(1))
                            {
                                count_ones=count_ones+1;
                            }
                        }
                    }
                    else
                    {
                        for (int i=MiningDataStream.Input_Stream.size()-1;i>=0;i--)
                        {   
                            if(MiningDataStream.Input_Stream.get(i).equals(1))
                            {
                                count_ones=count_ones+1;
                            }
                        }   
                    }
                    System.out.println("The number of ones of last "+query_bit+ " data is exact "+count_ones);
                }
                else
                {
                    //Calculate count from Bucket
                    long time_counter=MiningDataStream.timer_count-query_bit+1;
                    for (int i=MiningDataStream.Bucket.size()-1;i>=0;i--)
                    {   
                        if(time_counter>MiningDataStream.Bucket.get(i).get(0))
                        {
                            increment=true;
                            if (MiningDataStream.Bucket.get(i+1).get(1)!=1)
                            {
                                count_ones=count_ones-(MiningDataStream.Bucket.get(i+1).get(1))/2;
                            }
                            break;
                        }
                        else
                        {
                            /*
                            increment = false;
                            if(i!=0)
                            {
                                if(time_counter>MiningDataStream.Bucket.get(i-1).get(0))
                                {
                                    if(MiningDataStream.Bucket.get(i).get(1)!=1)
                                    {
                                        count_ones=count_ones+(MiningDataStream.Bucket.get(i).get(1)/2);
                                        increment=true;
                                    }
                                }
                            }
                            if(time_counter==MiningDataStream.Bucket.get(i).get(0)&& increment==false)
                            {
                                if(MiningDataStream.Bucket.get(i).get(1)!=1)
                                {
                                    count_ones=count_ones+(MiningDataStream.Bucket.get(i).get(1)/2);
                                    increment=true;
                                }
                            }
                            if (increment==false)
                            {
                                count_ones=count_ones+MiningDataStream.Bucket.get(i).get(1);
                            }*/
                            count_ones=count_ones+MiningDataStream.Bucket.get(i).get(1);
                        }
                    }
                    if (increment==false && (MiningDataStream.timer_count+1)>query_bit)
                    {
                        if(MiningDataStream.Bucket.get(0).get(1)!=1)
                        {
                            count_ones=count_ones-(MiningDataStream.Bucket.get(0).get(1))/2;
                        }
                    }
                    System.out.println("The number of ones of last "+query_bit+ " data is estimated "+count_ones);
                    //System.out.println(MiningDataStream.Bucket);
                }
                MiningDataStream.lock.release();
            }
        
        }catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
           e.printStackTrace();
        } 
        }
    }
}