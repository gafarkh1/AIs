using MPI;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

class gafarkh
{
    static void Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;

        using (new MPI.Environment(ref args))
        {
            Intracommunicator comm = Communicator.world;
            int rank = comm.Rank;
            int size = comm.Size;

            Queue<int> taskQueue = new Queue<int>();
            double deliveryTime = 0;
            double startTime = 0;

            if (rank == 0)
            {
                Console.Write("How many provinces? ");
                int provinces = int.Parse(Console.ReadLine());

                int taskID = 0;

                for (int i = 1; i <= provinces; i++)
                {
                    Console.Write($"[Province {i}] Number of pharmacies: ");
                    int pharmacies = int.Parse(Console.ReadLine());

                    Console.Write($"[Province {i}] Number of clinics: ");
                    int clinics = int.Parse(Console.ReadLine());

                    int total = pharmacies + clinics;
                    for (int j = 0; j < total; j++)
                        taskQueue.Enqueue(taskID++);
                }

                Console.Write("Average delivery time per task (in seconds): ");
                deliveryTime = double.Parse(Console.ReadLine());

                startTime = MPI.Environment.Time;

                int activeWorkers = size - 1;

                while (taskQueue.Count > 0 || activeWorkers > 0)
                {
                    Status status = comm.Probe(Communicator.anySource, 0);
                    int worker = status.Source;
                    comm.Receive<string>(worker, 0);

                    if (taskQueue.Count > 0)
                    {
                        int task = taskQueue.Dequeue();
                        comm.Send(task, worker, 1);
                    }
                    else
                    {
                        comm.Send(-1, worker, 1);
                        activeWorkers--;
                    }
                }

                double endTime = MPI.Environment.Time;
                double parallelTime = endTime - startTime;
                double serialTime = (taskID) * deliveryTime;

                Console.WriteLine($"Parallel execution time: {parallelTime:F2} seconds");
                Console.WriteLine($"Serial execution time: {serialTime:F2} seconds");
                Console.WriteLine($"Speedup: {serialTime / parallelTime:F2}x");
            }
            else
            {
                while (true)
                {
                    comm.Send("request", 0, 0);
                    int task = comm.Receive<int>(0, 1);

                    if (task == -1)
                        break;

                    string line = $"Worker {rank} is executing task #{task}{System.Environment.NewLine}";
                    File.AppendAllText($"output_worker_{rank}.txt", line, Encoding.UTF8);

                    Thread.Sleep((int)(deliveryTime * 1000));
                }
            }
        }
    }
}
