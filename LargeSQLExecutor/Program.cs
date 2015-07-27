using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LargeSQLExecutor
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }

        private static async Task MainAsync(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlConnectionStringBuilder scsb = new SqlConnectionStringBuilder();
            scsb.DataSource = ".";
            scsb.InitialCatalog = "Northwind";
            scsb.IntegratedSecurity = true;
            scsb.MaxPoolSize = 32767;
            scsb.ConnectTimeout = 600;
            string strConn = scsb.ConnectionString;
            ConcurrentBag<Task> tasks = new ConcurrentBag<Task>();
            int affectedRows = 0;
            foreach (var strCmd in File.ReadLines("LargeSQL.sql"))
            {
                tasks.Add(Task.Run(async () =>
                {
                Retry:
                    using (SqlConnection conn = new SqlConnection(strConn))
                    {
                        await conn.OpenAsync();
                        using (SqlCommand cmd = new SqlCommand(strCmd, conn))
                        {
                            cmd.CommandTimeout = 600;
                            try
                            {
                                int r = await cmd.ExecuteNonQueryAsync();
                                if (r > 0)
                                {
                                    Interlocked.Add(ref affectedRows, r);
                                }
                            }
                            catch (SqlException ex) when (ex.HResult == -2146232060)
                            {
                                // Network error
                                // System.Data.SqlClient.SqlException (0x80131904): A transport-level error has occurred when receiving results from the server.
                                // (provider: TCP Provider, error: 0 - The specified network name is no longer available.)
                                // ---> System.ComponentModel.Win32Exception (0x80004005): The specified network name is no longer available.
                                goto Retry;
                            }
                        }
                    }
                }));
            }

            // Task.WaitAll(tasks.ToArray()); will stuck
            foreach (var t in tasks)
            {
                try
                {
                    await t;
                }
                catch (Exception ex)
                {
                    // other error
                    File.AppendAllText("ex.log", DateTime.Now.ToString());
                    File.AppendAllText("ex.log", ex.ToString());
                    File.AppendAllText("ex.log", Environment.NewLine);
                }
            }
            sw.Stop();
            Console.WriteLine("Time: " + sw.Elapsed.ToString());
            Console.WriteLine("Affected rows: " + affectedRows);
            Console.ReadLine();
        }
    }
}