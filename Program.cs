using System;
using System.Threading.Tasks;
using MassTransit;
using MassTransit.QuartzIntegration;

namespace MassTransit.InMemory.Scheduling
{

    public class ScheduledMessageConsumer :
        IConsumer<ScheduledMessage>
    {
        public Task Consume(ConsumeContext<ScheduledMessage> context)
        {
            //Console.WriteLine("on ScheduledMessageConsumer...");
            return Console.Out.WriteAsync($"\r\n\r\n[ScheduledMessage] Received at: {DateTime.Now.ToString("s")}, Content: {context.Message.Message}\r\n> ");
        }
    }

    public class ScheduledMessage
    {
        public string Message { get; set; }
    }

	class Program
    {
        private const string appName = "MassTransit.InMemory.Scheduling";
        private const string options = "Options: [h]elp | [s]end scheduled message | [q]uit\r\n";
        private readonly static Uri epAddress = new Uri("rabbitmq://localhost");

        static void Main(string[] args)
        {
            OnInit();

            var bus = Bus.Factory.CreateUsingRabbitMq(cfg =>
            {
                var host = cfg.Host(epAddress, h =>
                {
                    h.Username("guest");
                    h.Password("guest");
                });

                // Handles the ScheduledMessage sent to the bus
                cfg.ReceiveEndpoint(host, "test_queue", e =>
                {
                    // if we wanted to handle published messages of type <ScheduledMessage>
                    //e.Handler<ScheduledMessage>(context =>
                    //{
                    //    return Console.Out.WriteAsync($"\r\n\r\n[ScheduledMessage] Received at: {DateTime.Now.ToString("s")}, Content: {context.Message.Message}\r\n");
                    //});

                    // registers a consumer for message <ScheduledMessage>
                    e.Consumer<ScheduledMessageConsumer>();
                });


                // Enable MassTransit's InMemory Scheduler
                // if empty, default queue == "quartz". I'm overriding to "scheduler_queue"
                cfg.UseInMemoryScheduler("scheduler_queue");
            });

            bus.Start();
            
            var run = true;
            while(run){
                Console.Write("> ");
                var cmd = Console.ReadLine();
                var slices = (cmd ?? "").Split(' ');

                switch(slices[0]){
                    case "q":
                        run = false;
                        break;

                    case "s":
                        if (slices.Length != 2){
                            Log(options);
                            continue;
                        }

                        var msg = new ScheduledMessage{ Message = $"Message '{slices[1]}' scheduled at {DateTime.Now.ToString("s")}" };

                        // if we wanted to send a message of type ScheduledMessage -----------------
                        // var sendEdpoint = bus.GetSendEndpoint(new Uri("rabbitmq://localhost/test_queue")).Result;
                        // sendEdpoint.Send(msg);
                        // --------------------------------------------------------------------------

                        var nextSchedule = DateTime.Now.AddSeconds(3);
                        Log($"Scheduled cmd to be execuded on: {nextSchedule}...");

                        // schedule bus message
                        var schedulerEndpoint = bus.GetSendEndpoint(new Uri("rabbitmq://localhost/scheduler_queue")).Result;  // scheduler queue
                        schedulerEndpoint.ScheduleSend(                 
                            new Uri("rabbitmq://localhost/test_queue"),  // bus queue
                            nextSchedule,
                            msg
                        );

                        break;

                    default:
                        Log(options);
                        break;
                }
            }

            bus.Stop();

            OnShutDown();
        }
        
        private static void OnInit()
        {
            Log("-----------------------------------------------");
            Log($"Initting {appName}...");
            Log("-----------------------------------------------\r\n");
        }

        private static void OnShutDown()
        {
            Log("\r\n-----------------------------------------------");
            Log($"Shutting down {appName}...");
            Log("-----------------------------------------------");
        }

        private static void Log(string msg)
        {
            Console.WriteLine(msg);
	    }
    }
}
