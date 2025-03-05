using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Transactions;

namespace NestedTransactionScope
{
    class Program
    {
        private const string _connectionString = "CONNECTION STRING HERE";

        public static void Main(string[] args)
        {
            MainTransactionScope();
            Console.ReadKey();
        }

        private static void MainTransactionScope()
        {
            DbConnection conexao;
            TransactionScope transaction = null;

            using (conexao = new SqlConnection(_connectionString))
            {
                try
                {
                    using (transaction = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted }))
                    {
                        if (conexao.State != ConnectionState.Open)
                            conexao.Open();

                        string sessionId = conexao.Query<string>("SELECT @@SPID").FirstOrDefault();
                        IEnumerable<long> transactionIds = conexao.Query<long>(@"
SELECT
    at.transaction_id
FROM
    sys.dm_tran_active_transactions at
JOIN
    sys.dm_tran_session_transactions st
ON
    at.transaction_id = st.transaction_id
WHERE
	st.session_id = @spid",
                        new
                        {
                            spid = sessionId
                        });

                        Console.WriteLine("- [MainTransactionScope]");
                        Console.WriteLine($"> SPID {sessionId}");
                        foreach (long item in transactionIds)
                        {
                            Console.WriteLine($">> TransactionID {item}");
                        }

                        try
                        {
                            //As seen on https://stackoverflow.com/a/4498115
                            //https://learn.microsoft.com/en-us/previous-versions/ms172152(v=vs.90)?redirectedfrom=MSDN
                            //Change the TransactionScopeOption based on usage
                            NestedWithTransactionScopeOption(TransactionScopeOption.Required);
                        }
                        catch (Exception exNested)
                        {
                            try
                            {
                                //When the 'NestedWithTransactionScopeOption' method is called with 'TransactionScopeOption.Required', the query below will throw an exception
                                long object_id = conexao.QueryFirstOrDefault<long>("SELECT TOP 1 object_id FROM sys.objects");

                                Console.WriteLine($"Id {object_id}. {exNested.Message}");
                            }
                            catch (Exception ex2)
                            {
                                Console.WriteLine("");
                                Console.WriteLine(ex2.Message);
                            }
                        }

                        transaction.Complete();
                    }
                }
                catch (Exception ex)
                {
                    if (transaction != null)
                    {
                        transaction.Dispose();
                    }

                    Console.WriteLine(ex.Message);
                }
            }
        }

        private static void NestedWithTransactionScopeOption(TransactionScopeOption option)
        {
            using (DbConnection conexao = new SqlConnection(_connectionString))
            {
                using (TransactionScope transaction = new TransactionScope(option, new TransactionOptions { IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted, Timeout = new TimeSpan(0, 25, 0) }))
                {
                    try
                    {
                        if (conexao.State != ConnectionState.Open)
                            conexao.Open();

                        string sessionId = conexao.Query<string>("SELECT @@SPID").FirstOrDefault();
                        IEnumerable<long> transactionIds = conexao.Query<long>(@"
SELECT
    at.transaction_id
FROM
    sys.dm_tran_active_transactions at
JOIN
    sys.dm_tran_session_transactions st
ON
    at.transaction_id = st.transaction_id
WHERE
	st.session_id = @spid",
                        new
                        {
                            spid = sessionId
                        });

                        Console.WriteLine("");
                        Console.WriteLine("- [NestedWithTransactionScopeOption]");
                        Console.WriteLine($"> SPID {sessionId}");
                        foreach (long item in transactionIds)
                        {
                            Console.WriteLine($">> TransactionID {item}");
                        }

                        throw new Exception("Simulated Error");

                        //This will never complete, just ignore for the sake of the example.
                        transaction.Complete();
                    }
                    catch (Exception ex)
                    {
                        if (transaction != null)
                        {
                            transaction.Dispose();
                        }

                        throw new Exception("Error inside NestedWithTransactionScopeOption", ex);
                    }
                }
            }
        }
    }
}
