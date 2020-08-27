using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Backend
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Started");
            Console.WriteLine("Getting DB Connection...");

            // Instantiate new list of tuples to house the results from sql query
            List<Tuple<string, int>> baseCountryPopulation = GetCountryPopulationsFromSQLAsync().Result;

            // Retrive other tuple list
            ConcreteStatService concreteStatService = new ConcreteStatService();
            List<Tuple<string, int>> countryPopulationFromApi = concreteStatService.GetCountryPopulationsAsync().Result;
            // Merge the tuples with the sql data as the base
            baseCountryPopulation = MergeTupleLists(baseCountryPopulation, countryPopulationFromApi).Result;
            
            var sorted = baseCountryPopulation.OrderBy(t => t.Item1).ToList();
            foreach (var tuple in sorted)
            {
                Console.WriteLine("{0} - {1}", tuple.Item1, tuple.Item2);
            }
            Console.ReadLine();
        }

        public static Task<List<Tuple<string, int>>> GetCountryPopulationsFromSQLAsync()
        {
            IDbManager db = new SqliteDbManager();
            DbConnection conn = db.getConnection();

            if (conn == null)
            {
                Console.WriteLine("Failed to get connection");
            }
            List<Tuple<string, int>> countryPopulationFromSql = new List<Tuple<string, int>>();

            // Retrive distince records while joining the country, state, and city tables. Return CountryName and SUM of city.population
            string stm = "SELECT DISTINCT country.CountryName, SUM(city.population) AS Population FROM country JOIN state ON state.countryId = country.countryId JOIN city ON city.stateId = state.stateId GROUP BY country.CountryName;";

            using var cmd = new SQLiteCommand(stm, (SQLiteConnection)conn);
            using SQLiteDataReader rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                // Build new tuple items and add to list
                string countryName = (string)rdr[0];
                int population = Convert.ToInt32(rdr[1]);
                Tuple<string, int> item = new Tuple<string, int>(countryName, population);
                countryPopulationFromSql.Add(item);
            }
            rdr.Close();

            return Task.FromResult<List<Tuple<string, int>>>(countryPopulationFromSql);
        }

        public static Task<List<Tuple<string, int>>> MergeTupleLists(List<Tuple<string, int>> baseTuple, List<Tuple<string, int>> additionalTuple)
        {
            foreach (var item in additionalTuple)
            {
                if (!baseTuple.Any(m => m.Item1 == item.Item1))
                {
                    baseTuple.Add(item);
                }
            }
            return Task.FromResult<List<Tuple<string, int>>>(baseTuple);
        }

    }
}
