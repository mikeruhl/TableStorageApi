using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.CosmosDB.Table;
using Microsoft.Azure.Storage;

namespace TableStorageApi
{
    class Program
    {
        static void Main(string[] args)
        {
            var cStr = ConfigurationManager.ConnectionStrings["storageAccount"].ConnectionString;

            if (string.IsNullOrEmpty(cStr))
            {
                Console.WriteLine("Please insert your storage account connection string before continuing.");
                Environment.Exit(1);
            }
                

            // Retrieve the storage account from the connection string.
            var storageAccount = CloudStorageAccount.Parse(cStr);

            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Retrieve a reference to the table.
            CloudTable table = tableClient.GetTableReference("people");

            // Create the table if it doesn't exist.
            table.CreateIfNotExists();

            //Wipe contents from previous run
            Console.WriteLine("Wiping contents from any previous runs");
            var deleted = DeleteAllEntities(table);
            Console.WriteLine($"Deleted {deleted} entries");
            Console.WriteLine();

            Console.WriteLine("Inserting one record...");
            var customer1 = (CustomerEntity)InsertOneRecord(table).Result;
            Console.WriteLine($"Inserted One Record with Partition Key: {customer1.PartitionKey}");
            Console.WriteLine();

            Console.WriteLine("Inserting batch records...");
            var multipleCustomers = InsertBatchRecords(table).Select(f => (CustomerEntity)f.Result);
            Console.WriteLine($"Inserted {multipleCustomers.Count()} records");
            Console.WriteLine();

            Console.WriteLine("Retrieving a range of entities in a partition");
            var rangeQuery = RetrieveRangeOfEntitiesInPartition(table);
            Console.WriteLine("Retrieved range results");
            foreach (var entity in rangeQuery)
                Console.WriteLine("\t{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
            Console.WriteLine();

            Console.WriteLine("Retrieve a single entity");
            var singleEntity = RetrieveASingleEntity(table);
            if (singleEntity != null)
                Console.WriteLine($"Retrieved single entity with phone: {singleEntity.PhoneNumber}");
            else
                Console.WriteLine("Could not retrieve single entity");
            Console.WriteLine();

            Console.WriteLine("Replacing an entity");
            var replaced = ReplaceAnEntity(table);
            if (replaced != null)
                Console.WriteLine($"Replaced an entity with a new phone number: {replaced.PhoneNumber}");
            else
                Console.WriteLine("Could not find entity to replace property");
            Console.WriteLine();

            Console.WriteLine("Inserting or replacing entity");
            var insertOrReplaced = InsertOrReplaceEntity(table);
            Console.WriteLine($"We changed phone: {insertOrReplaced.Item1.PhoneNumber} to phone: {insertOrReplaced.Item2.PhoneNumber}");
            Console.WriteLine();

            Console.WriteLine("Query a subset of entity properties");
            var querySubset = QueryASubsetOfEntityProperties(table);
            Console.WriteLine($"Queried Subset and got all these numbers: {querySubset}");
            Console.WriteLine();

            Console.WriteLine("Delete an Entity");
            Console.WriteLine($"Response from Entity Deletion: {DeleteAnEntity(table)}");
            Console.WriteLine();

            Console.WriteLine("Delete a table");
            DeleteATable(table);
            Console.WriteLine($"Table exists == true? {table.Exists()}");
        }

        private static TableResult InsertOneRecord(CloudTable table)
        {
            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity("Harp", "Walter");
            customer1.Email = "Walter@contoso.com";
            customer1.PhoneNumber = "425-555-0101";

            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer1);

            // Execute the insert operation.
            return table.Execute(insertOperation);
        }

        private static IList<TableResult> InsertBatchRecords(CloudTable table)
        {
            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Create a customer entity and add it to the table.
            CustomerEntity customer1 = new CustomerEntity("Smith", "Jeff");
            customer1.Email = "Jeff@contoso.com";
            customer1.PhoneNumber = "425-555-0104";

            // Create another customer entity and add it to the table.
            CustomerEntity customer2 = new CustomerEntity("Smith", "Ben");
            customer2.Email = "Ben@contoso.com";
            customer2.PhoneNumber = "425-555-0102";

            // Add both customer entities to the batch insert operation.
            batchOperation.Insert(customer1);
            batchOperation.Insert(customer2);

            // Execute the batch operation.
            return table.ExecuteBatch(batchOperation);
        }

        private static IEnumerable<CustomerEntity> RetrieveRangeOfEntitiesInPartition(CloudTable table)
        {

            // Create the table query.
            TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, "E")));

            return table.ExecuteQuery(rangeQuery);
        }

        private static CustomerEntity RetrieveASingleEntity(CloudTable table)
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            return (CustomerEntity)retrievedResult.Result;
        }

        private static CustomerEntity ReplaceAnEntity(CloudTable table)
        {
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity object.
            CustomerEntity updateEntity = (CustomerEntity)retrievedResult.Result;

            if (updateEntity != null)
            {
                // Change the phone number.
                updateEntity.PhoneNumber = "425-555-0105";

                // Create the Replace TableOperation.
                TableOperation updateOperation = TableOperation.Replace(updateEntity);

                // Execute the operation.
                return (CustomerEntity)table.Execute(updateOperation).Result;
            }

            return null;
        }

        private static Tuple<CustomerEntity, CustomerEntity> InsertOrReplaceEntity(CloudTable table)
        {
            // Create a customer entity.
            CustomerEntity customer3 = new CustomerEntity("Jones", "Fred");
            customer3.Email = "Fred@contoso.com";
            customer3.PhoneNumber = "425-555-0106";

            // Create the TableOperation object that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer3);

            // Execute the operation.
            var item1 = (CustomerEntity)table.Execute(insertOperation).Result;

            // Create another customer entity with the same partition key and row key.
            // We've already created a 'Fred Jones' entity and saved it to the
            // 'people' table, but here we're specifying a different value for the
            // PhoneNumber property.
            CustomerEntity customer4 = new CustomerEntity("Jones", "Fred");
            customer4.Email = "Fred@contoso.com";
            customer4.PhoneNumber = "425-555-0107";

            // Create the InsertOrReplace TableOperation.
            TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(customer4);

            // Execute the operation. Because a 'Fred Jones' entity already exists in the
            // 'people' table, its property values will be overwritten by those in this
            // CustomerEntity. If 'Fred Jones' didn't already exist, the entity would be
            // added to the table.
            var item2 = (CustomerEntity)table.Execute(insertOrReplaceOperation).Result;

            return new Tuple<CustomerEntity, CustomerEntity>(item1, item2);
        }

        private static string QueryASubsetOfEntityProperties(CloudTable table)
        {
            // Define the query, and select only the Email property.
            TableQuery<DynamicTableEntity> projectionQuery = new TableQuery<DynamicTableEntity>().Select(new string[] { "Email" });

            // Define an entity resolver to work with the entity after retrieval.
            EntityResolver<string> resolver = (pk, rk, ts, props, etag) => props.ContainsKey("Email") ? props["Email"].StringValue : null;

            var responseList = new List<string>();
            foreach (string projectedEmail in table.ExecuteQuery(projectionQuery, resolver, null, null))
            {
                responseList.Add(projectedEmail);
            }

            return string.Join(",", responseList);
        }

        private static string DeleteAnEntity(CloudTable table)
        {
            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith", "Ben");

            // Execute the operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Assign the result to a CustomerEntity.
            CustomerEntity deleteEntity = (CustomerEntity)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.Execute(deleteOperation);

                return "Entity deleted.";
            }
            else
            {
                return "Could not retrieve the entity.";
            }
        }

        static void DeleteATable(CloudTable table)
        {
            // Delete the table it if exists.
            table.DeleteIfExists();
        }

        static int DeleteAllEntities(CloudTable table)
        {
            var batchOperation = new TableBatchOperation();
            var entities = table.CreateQuery<CustomerEntity>().ToList();
            if (!entities.Any())
                return 0;

            foreach (var customer in entities)
                batchOperation.Add(TableOperation.Delete(customer));

            table.ExecuteBatch(batchOperation);
            return entities.Count;
        }
    }
}