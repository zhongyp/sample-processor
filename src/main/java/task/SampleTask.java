package task;

import javafx.scene.control.Alert;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * @author yupeng chung <yupengchung@gmail.com>
 * @version 1.0
 * @date 2022/5/27
 * @since jdk1.8
 */
public class SampleTask {

    public static void main(String[] args) {

        System.out.println("I started !");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> transactions = env
                .addSource(new LocalDataSource())
                .name("streams data");

        DataStream<Alert> alerts = transactions
                .process(new LocalDataProcessor())
                .name("processor");

        alerts.addSink(new AlertSink())
                .name("send-alerts");

        try {
            env.execute("job executor");
            System.out.println("I ended !");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


class LocalDataSource implements SourceFunction<Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        int integer = 0;
        while (true) {
            Thread.sleep(1000);
            System.out.println("I got it :" + integer++);
            sourceContext.collect(integer);
        }
    }

    @Override
    public void cancel() {

    }
}

class LocalDataProcessor extends ProcessFunction<Integer, Alert> {
    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(Integer integer, Context context, Collector<Alert> collector) throws Exception {
        System.out.println("I process it: " + integer);
    }
}

class AlertSink implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    @Override
    public void invoke(Alert value, Context context) {
        System.out.println("I sink it: " + value);
    }
}