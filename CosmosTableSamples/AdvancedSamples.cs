using System;
using System.Collections.Generic;

namespace CosmosTableSamples
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Table;
    using Model;

    class AdvancedSamples
    {
        public async Task RunSamples()
        {
            Console.WriteLine("Azure Cosmos DB 表 - 高级示例\n");
            Console.WriteLine();

            string tableName = "demo" + Guid.NewGuid().ToString().Substring(0, 5);

            // 创建或引用现有表
            CloudTable table = await Common.CreateTableAsync(tableName);
            CloudTableClient tableClient = table.ServiceClient;

            try
            {
                // 演示批处理操作和分段多实体查询等高级功能
                await AdvancedDataOperationsAsync(table);

                // 列出帐户中的表
                await TableListingOperations(tableClient);

                if (!SamplesUtils.IsAzureCosmosdbTable())
                {
                    // 创建一个 SAS 并尝试使用 SAS 进行 CRUD 操作。
                    await AdvancedDataOperationsWithSasAsync(table);

                    // 服务属性
                    await ServicePropertiesSample(tableClient);

                    // CORS
                    await CorsSample(tableClient);

                    // 服务统计
                    await ServiceStatsSample(tableClient);

                    // 表 Acl
                    await TableAclSample(table);

                    // 创建 SAS 并尝试使用 SAS 和表上的共享访问策略进行 CRUD 操作。
                    await AdvancedDataOperationsWithSasAndSharedAccessPolicyOnTableAsync(table);
                }
            }
            finally
            {
                // 删除表
                await table.DeleteIfExistsAsync();
            }
        }

        private static async Task AdvancedDataOperationsAsync(CloudTable table)
        {
            // 演示 upsert 和批处理表操作
            Console.WriteLine("插入一批实体。 ");
            await BatchInsertOfCustomerEntitiesAsync(table, "Smith");
            Console.WriteLine();

            // 使用简单查询查询分区内的一系列数据
            Console.WriteLine("检索姓氏为 Smith 且names  >= 1 且 <= 75 的实体");
            ExecuteSimpleQuery(table, "Smith", "0001", "0075");
            Console.WriteLine();

            // 查询一个分区内相同范围的数据，一次返回50个实体的结果段
            Console.WriteLine("检索姓氏为 Smith 且名字 >= 1 且 <= 75 的实体");
            await PartitionRangeQueryAsync(table, "Smith", "0001", "0075");
            Console.WriteLine();

            // 查询一个分区内的所有数据
            Console.WriteLine("检索姓氏为 Smith 的实体。");
            await PartitionScanAsync(table, "Smith");
            Console.WriteLine();

            if (SamplesUtils.IsAzureCosmosdbTable())
            {
                // 演示 upsert 和批处理表操作
                Console.WriteLine("插入一批实体。 ");
                await BatchInsertOfCustomerEntitiesAsync(table, "Dave");
                Console.WriteLine();

                // 演示 upsert 和批处理表操作
                Console.WriteLine("插入一批实体。 ");
                await BatchInsertOfCustomerEntitiesAsync(table, "Shirly");
                Console.WriteLine();

                // 按顺序查询所有数据跨分区
                Console.WriteLine("按跨分区顺序查询");
                await ExecuteCrossPartitionQueryWithOrderBy(table, "0001", "0025");
                Console.WriteLine();
            }
        }

        /// <summary>
        /// 列出帐户中的表。
        /// </summary>
        /// <param name="tableClient">表客户端。</param>
        /// <returns>A Task object</returns>
        private static async Task TableListingOperations(CloudTableClient tableClient)
        {
            try
            {
                // 要列出帐户中的所有表，请取消注释以下行。
                // 请注意，如果帐户包含大量表，则列出帐户中的所有表可能需要很长时间。
                // ListAllTables(tableClient);

                // 列出以指定前缀开头的表。
                await ListTablesWithPrefix(tableClient, "demo");
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 列出帐户中名称以指定前缀开头的表。
        /// </summary>
        /// <param name="tableClient">表服务客户端对象。</param>
        /// <param name="prefix">表名前缀。</param>
        /// <returns>A Task object</returns>
        private static async Task ListTablesWithPrefix(CloudTableClient tableClient, string prefix)
        {
            Console.WriteLine("列出所有以前缀 {0} 开头的表：", prefix);

            TableContinuationToken continuationToken = null;
            TableResultSegment resultSegment = null;

            try
            {
                do
                {
                    // 列出以指定前缀开头的表。
                    // 为 maxResults 参数传入 null 将返回最大结果数（最多 5000）。
                    resultSegment = await tableClient.ListTablesSegmentedAsync(
                        prefix, null, continuationToken, null, null);

                    // 枚举返回的表。
                    foreach (var table in resultSegment.Results)
                    {
                        Console.WriteLine("\t表：" + table.Name);
                    }
                }
                while (continuationToken != null);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 列出帐户中的所有表。
        /// </summary>
        /// <param name="tableClient">表服务客户端对象。</param>
        private static void ListAllTables(CloudTableClient tableClient)
        {
            Console.WriteLine("列出帐户中的所有表：");

            try
            {
                // 请注意，如果帐户包含大量表，则列出帐户中的所有表可能需要很长时间。
                foreach (var table in tableClient.ListTables())
                {
                    Console.WriteLine("\t表：" + table.Name);
                }

                Console.WriteLine();
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 演示插入大量实体。 批处理操作的一些注意事项：
        ///  1. 您可以在同一个批处理操作中执行更新、删除和插入。
        ///  2. 单个批处理操作最多可以包含 100 个实体。
        ///  3. 单个批处理操作中的所有实体必须具有相同的分区键。
        ///  4. 虽然可以将查询作为批处理操作执行，但它必须是批处理中的唯一操作。
        ///  5. 批处理大小必须小于或等于 2 MB
        /// </summary>
        /// <param name="table">示例表名</param>
        /// <param name="partitionKey">实体的分区</param>
        /// <returns>A Task object</returns>
        private static async Task BatchInsertOfCustomerEntitiesAsync(CloudTable table, string partitionKey)
        {
            try
            {
                // 创建批处理操作。
                TableBatchOperation batchOperation = new TableBatchOperation();

                // 以下代码生成在查询示例期间使用的测试数据。
                for (int i = 0; i < 100; i++)
                {
                    batchOperation.InsertOrMerge(new CustomerEntity(partitionKey, string.Format("{0}", i.ToString("D4")))
                    {
                        Email = string.Format("{0}@contoso.com", i.ToString("D4")),
                        PhoneNumber = string.Format("425-555-{0}", i.ToString("D4"))
                    });
                }

                // 执行批处理操作。
                TableBatchResult results = await table.ExecuteBatchAsync(batchOperation);
                foreach (var res in results)
                {
                    var customerInserted = res.Result as CustomerEntity;
                    Console.WriteLine("插入的实体具有\t Etag = {0} 和 PartitionKey = {1}，RowKey = {2}", customerInserted.ETag, customerInserted.PartitionKey, customerInserted.RowKey);
                }

                if (results.RequestCharge.HasValue)
                {
                    Console.WriteLine("请求对 Cosmos DB 表进行批量操作的费用：" + results.RequestCharge);
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 演示分区范围查询，该查询在分区内搜索特定范围内的一组实体。 
        /// 此查询返回范围内的所有实体。 请注意，如果您的表包含大量数据，则查询可能会很慢或可能会超时。 
        /// 在这种情况下，请使用分段查询，如 PartitionRangeQueryAsync() 示例方法中所示。
        /// 请注意，出于示例的目的，同步调用 ExecuteSimpleQuery 方法。 
        /// 但是，在使用 async/await 模式的实际应用程序中，最佳实践建议始终使用异步方法。
        /// </summary>
        /// <param name="table">示例表名</param>
        /// <param name="partitionKey">要搜索的分区</param>
        /// <param name="startRowKey">要搜索的行键范围的下限</param>
        /// <param name="endRowKey">要搜索的行键范围的上限</param>
        private static void ExecuteSimpleQuery(CloudTable table, string partitionKey, string startRowKey,
            string endRowKey)
        {
            try
            {
                // 使用流体 API 创建范围查询
                TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual,
                                startRowKey),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                                endRowKey))));

                foreach (CustomerEntity entity in table.ExecuteQuery(rangeQuery))
                {
                    Console.WriteLine("Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email,
                        entity.PhoneNumber);
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 演示分区范围查询，该查询在分区内搜索特定范围内的一组实体。 异步 API 要求用户处理段大小并使用延续令牌返回下一个段。
        /// </summary>
        /// <param name="table">示例表名</param>
        /// <param name="partitionKey">要搜索的分区</param>
        /// <param name="startRowKey">要搜索的行键范围的下限</param>
        /// <param name="endRowKey">要搜索的行键范围的上限</param>
        /// <returns>A Task object</returns>
        private static async Task PartitionRangeQueryAsync(CloudTable table, string partitionKey, string startRowKey, string endRowKey)
        {
            try
            {
                // 使用流体 API 创建范围查询
                TableQuery<CustomerEntity> rangeQuery = new TableQuery<CustomerEntity>().Where(
                    TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                            TableOperators.And,
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey))));

                // 一次从服务器请求 50 个结果。
                TableContinuationToken token = null;
                rangeQuery.TakeCount = 50;
                int segmentNumber = 0;
                do
                {
                    // 执行查询，传入继续令牌。
                    // 第一次调用此方法时，延续标记为空。 如果有更多结果，则调用会填充继续标记以供下一次调用使用。
                    TableQuerySegment<CustomerEntity> segment = await table.ExecuteQuerySegmentedAsync(rangeQuery, token);

                    // 指示正在显示哪个段
                    if (segment.Results.Count > 0)
                    {
                        segmentNumber++;
                        Console.WriteLine();
                        Console.WriteLine("部分 {0}", segmentNumber);
                    }

                    // 为下一次调用 ExecuteQuerySegmentedAsync 保存继续令牌
                    token = segment.ContinuationToken;

                    // 写出返回的每个实体的属性。
                    foreach (CustomerEntity entity in segment)
                    {
                        Console.WriteLine("\t Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
                    }

                    Console.WriteLine();
                }
                while (token != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 演示分区扫描，我们正在搜索分区内的所有实体。 请注意，这不如范围扫描有效 - 但绝对比全表扫描更有效。 
        /// 异步 API 要求用户处理段大小并使用延续令牌返回下一个段。
        /// </summary>
        /// <param name="table">示例表名</param>
        /// <param name="partitionKey">要搜索的分区</param>
        /// <returns>A Task object</returns>
        private static async Task PartitionScanAsync(CloudTable table, string partitionKey)
        {
            try
            {
                TableQuery<CustomerEntity> partitionScanQuery =
                    new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

                TableContinuationToken token = null;

                // 从每个查询段读取实体。
                do
                {
                    TableQuerySegment<CustomerEntity> segment = await table.ExecuteQuerySegmentedAsync(partitionScanQuery, token);
                    token = segment.ContinuationToken;
                    foreach (CustomerEntity entity in segment)
                    {
                        Console.WriteLine("Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
                    }
                }
                while (token != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 针对 Cosmos Table API 演示带有 order by 的跨分区查询
        /// </summary>
        /// <param name="table">示例表名</param>
        /// <param name="startRowKey"> 要搜索的行键范围的下限</param>
        /// <param name="endRowKey">要搜索的行键范围的上限</param>
        /// <returns>A Task object</returns>
        private static async Task ExecuteCrossPartitionQueryWithOrderBy(CloudTable table, string startRowKey, string endRowKey)
        {
            try
            {
                TableQuery<CustomerEntity> partitionScanQuery =
                    new TableQuery<CustomerEntity>().Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRowKey),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRowKey)
                            )
                        )
                        .OrderBy("RowKey");

                TableContinuationToken token = null;

                // 从每个查询段读取实体。
                do
                {
                    TableQuerySegment<CustomerEntity> segment = await table.ExecuteQuerySegmentedAsync(partitionScanQuery, token);

                    if (segment.RequestCharge.HasValue)
                    {
                        Console.WriteLine("查询操作的请求费用： " + segment.RequestCharge);
                    }

                    token = segment.ContinuationToken;
                    foreach (CustomerEntity entity in segment)
                    {
                        Console.WriteLine("Customer: {0},{1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey, entity.Email, entity.PhoneNumber);
                    }
                }
                while (token != null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 管理表服务的属性。
        /// </summary>
        /// <param name="tableClient"></param>
        private static async Task ServicePropertiesSample(CloudTableClient tableClient)
        {
            Console.WriteLine();

            // 获取服务属性
            Console.WriteLine("获取服务属性");
            ServiceProperties originalProperties = await tableClient.GetServicePropertiesAsync();
            try
            {
                // 设置服务属性
                Console.WriteLine("设置服务属性");

                ServiceProperties props = await tableClient.GetServicePropertiesAsync();
                props.Logging.LoggingOperations = LoggingOperations.Read | LoggingOperations.Write;
                props.Logging.RetentionDays = 5;
                props.Logging.Version = "1.0";

                props.HourMetrics.MetricsLevel = MetricsLevel.Service;
                props.HourMetrics.RetentionDays = 6;
                props.HourMetrics.Version = "1.0";

                props.MinuteMetrics.MetricsLevel = MetricsLevel.Service;
                props.MinuteMetrics.RetentionDays = 6;
                props.MinuteMetrics.Version = "1.0";

                await tableClient.SetServicePropertiesAsync(props);
            }
            finally
            {
                // 恢复到原始服务属性
                Console.WriteLine("恢复到原始服务属性");
                await tableClient.SetServicePropertiesAsync(originalProperties);
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 查询Table服务的跨域资源共享（CORS）规则
        /// </summary>
        /// <param name="tableClient"></param>
        private static async Task CorsSample(CloudTableClient tableClient)
        {
            Console.WriteLine();

            // 获取服务属性
            Console.WriteLine("获取服务属性");
            ServiceProperties originalProperties = await tableClient.GetServicePropertiesAsync();
            try
            {
                // 添加 CORS 规则
                Console.WriteLine("添加 CORS 规则");

                CorsRule corsRule = new CorsRule
                {
                    AllowedHeaders = new List<string> { "*" },
                    AllowedMethods = CorsHttpMethods.Get,
                    AllowedOrigins = new List<string> { "*" },
                    ExposedHeaders = new List<string> { "*" },
                    MaxAgeInSeconds = 3600
                };

                ServiceProperties serviceProperties = await tableClient.GetServicePropertiesAsync();
                serviceProperties.Cors.CorsRules.Add(corsRule);
                await tableClient.SetServicePropertiesAsync(serviceProperties);
            }
            finally
            {
                // 恢复到原始服务属性
                Console.WriteLine("恢复到原始服务属性");
                await tableClient.SetServicePropertiesAsync(originalProperties);
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 检索与 Table 服务的复制相关的统计信息
        /// </summary>
        /// <param name="tableClient"></param>
        private static async Task ServiceStatsSample(CloudTableClient tableClient)
        {
            Console.WriteLine();

            var originalLocation = tableClient.DefaultRequestOptions.LocationMode;

            Console.WriteLine("服务统计：");
            try
            {
                tableClient.DefaultRequestOptions.LocationMode = LocationMode.SecondaryOnly;
                ServiceStats stats = await tableClient.GetServiceStatsAsync();
                Console.WriteLine("    上次同步时间：{0}", stats.GeoReplication.LastSyncTime);
                Console.WriteLine("    状态：{0}", stats.GeoReplication.Status);
            }
            catch (StorageException)
            {
                // 仅适用于 RA-GRS（读取访问 - 地理冗余存储）
            }
            finally
            {
                // 恢复原值
                tableClient.DefaultRequestOptions.LocationMode = originalLocation;
            }

            Console.WriteLine();
        }

        /// <summary>
        /// 管理表上指定的存储访问策略
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        private static async Task TableAclSample(CloudTable table)
        {
            // 设置表权限
            SharedAccessTablePolicy accessTablePolicy = new SharedAccessTablePolicy();
            accessTablePolicy.SharedAccessStartTime = new DateTimeOffset(DateTime.Now);
            accessTablePolicy.SharedAccessExpiryTime = new DateTimeOffset(DateTime.Now.AddMinutes(10));
            accessTablePolicy.Permissions = SharedAccessTablePermissions.Update;
            TablePermissions permissions = new TablePermissions();
            permissions.SharedAccessPolicies.Add("key1", accessTablePolicy);
            Console.WriteLine("设置表权限");
            await table.SetPermissionsAsync(permissions);

            // 获取表权限
            Console.WriteLine("获取表权限");
            permissions = await table.GetPermissionsAsync();
            foreach (var keyValue in permissions.SharedAccessPolicies)
            {
                Console.WriteLine("  {0}:", keyValue.Key);
                Console.WriteLine("    权限： {0}:", keyValue.Value.Permissions);
                Console.WriteLine("    开始时间： {0}:", keyValue.Value.SharedAccessStartTime);
                Console.WriteLine("    到期时间： {0}:", keyValue.Value.SharedAccessExpiryTime);
            }
        }

        /// <summary>
        /// 演示使用 SAS 进行身份验证的基本 CRUD 操作。
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>A Task object</returns>
        private static async Task AdvancedDataOperationsWithSasAsync(CloudTable table)
        {
            // 在表上生成临时 SAS，然后测试 SAS。 它允许对表进行所有 CRUD 操作。
            string adHocTableSAS = GetTableSasUri(table);

            // 创建客户实体的实例。
            CustomerEntity customer1 = new CustomerEntity("Johnson", "Mary")
            {
                Email = "mary@contoso.com",
                PhoneNumber = "425-555-0105"
            };
            await TestTableSAS(adHocTableSAS, customer1);
        }


        /// <summary>
        /// 演示使用 SAS 进行身份验证的基本 CRUD 操作。
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>A Task object</returns>
        private static async Task AdvancedDataOperationsWithSasAndSharedAccessPolicyOnTableAsync(CloudTable table)
        {
            string sharedAccessPolicyName = "sample-policy";

            // 在表上创建共享访问策略。
            // 可以选择使用访问策略来为表上的共享访问签名提供约束。
            await CreateSharedAccessPolicy(table, sharedAccessPolicyName);

            // 为表生成 SAS URI，使用存储的访问策略在 SAS 上设置约束。
            // 然后测试SAS。 所有 CRUD 操作都应该成功。
            string sharedPolicyTableSAS = GetTableSasUri(table, sharedAccessPolicyName);

            // 创建客户实体的实例。
            CustomerEntity customer2 = new CustomerEntity("Wilson", "Joe")
            {
                Email = "joe@contoso.com",
                PhoneNumber = "425-555-0106"
            };
            await TestTableSAS(sharedPolicyTableSAS, customer2);
        }

        /// <summary>
        /// 返回包含表的 SAS 的 URI。
        /// </summary>
        /// <param name="table">一个 CloudTable 对象。</param>
        /// <param name="storedPolicyName">包含存储访问策略名称的字符串。 如果为 null，则创建一个临时 SAS。</param>
        /// <returns>包含表的 URI 的字符串，并附加了 SAS 令牌。</returns>
        private static string GetTableSasUri(CloudTable table, string storedPolicyName = null)
        {
            string sasTableToken;

            // 如果未指定存储策略，则创建新的访问策略并定义其约束。
            if (storedPolicyName == null)
            {
                //请注意，SharedAccessTablePolicy 类既用于定义 ad-hoc SAS 的参数，也用于构造保存到表的共享访问策略的共享访问策略。
                SharedAccessTablePolicy adHocPolicy = new SharedAccessTablePolicy()
                {
                    // 权限使用户能够添加、更新、查询和删除表中的实体。
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                    Permissions = SharedAccessTablePermissions.Add | SharedAccessTablePermissions.Update |
                        SharedAccessTablePermissions.Query | SharedAccessTablePermissions.Delete
                };

                // 在表上生成共享访问签名，直接在签名上设置约束。
                sasTableToken = table.GetSharedAccessSignature(adHocPolicy, null);

                Console.WriteLine("表的 SAS（临时）：{0}", sasTableToken);
                Console.WriteLine();
            }
            else
            {
                // 在表上生成共享访问签名。 在这种情况下，共享访问签名的所有约束都在存储的访问策略中指定，该策略由名称提供。
                // 还可以在 ad-hoc SAS 上指定一些约束，在存储的访问策略上指定其他约束。
                // 但是，必须对其中一个指定约束； 不能在两者上都指定。
                sasTableToken = table.GetSharedAccessSignature(null, storedPolicyName);

                Console.WriteLine("SAS 表（存储访问策略）：{0}", sasTableToken);
                Console.WriteLine();
            }

            // 返回表的 URI 字符串，包括 SAS 令牌。
            return table.Uri + sasTableToken;
        }

        /// <summary>
        /// 在表上创建共享访问策略。
        /// </summary>
        /// <param name="table">一个 CloudTable 对象。</param>
        /// <param name="policyName">存储的访问策略的名称。</param>
        /// <returns>A Task object</returns>
        private static async Task CreateSharedAccessPolicy(CloudTable table, string policyName)
        {
            // 创建新的共享访问策略并定义其约束。
            // 访问策略提供添加、更新和查询权限。
            SharedAccessTablePolicy sharedPolicy = new SharedAccessTablePolicy()
            {
                // 权限使用户能够添加、更新、查询和删除表中的实体。
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                Permissions = SharedAccessTablePermissions.Add | SharedAccessTablePermissions.Update |
                        SharedAccessTablePermissions.Query | SharedAccessTablePermissions.Delete
            };

            try
            {
                // 获取表的现有权限。
                TablePermissions permissions = await table.GetPermissionsAsync();

                // 将新策略添加到表的权限，并更新表的权限。
                permissions.SharedAccessPolicies.Add(policyName, sharedPolicy);
                await table.SetPermissionsAsync(permissions);

                Console.WriteLine("等待 30 秒以传播权限");
                Thread.Sleep(30);
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }

        /// <summary>
        /// 测试表 SAS 以确定它允许哪些操作。
        /// </summary>
        /// <param name="sasUri">包含附加了 SAS 的 URI 的字符串。</param>
        /// <param name="customer">客户实体。</param>
        /// <returns>A Task object</returns>
        private static async Task TestTableSAS(string sasUri, CustomerEntity customer)
        {
            // 尝试使用提供的 SAS 执行表操作。
            // 请注意，此处不需要存储帐户凭据； SAS 在 URI 上提供必要的身份验证信息。

            // 使用 SAS URI 返回对表的引用。
            CloudTable table = new CloudTable(new Uri(sasUri));

            // Upsert（添加/更新）操作：插入一个实体。
            // 此操作需要对 SAS 的添加和更新权限。
            try
            {
                // 插入新实体。
                customer = await SamplesUtils.InsertOrMergeEntityAsync(table, customer);

                Console.WriteLine("SAS {0} 的添加操作成功", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("SAS {0} 的添加操作失败", sasUri);
                    Console.WriteLine("附加错误信息：" + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            // 读操作：查询一个实体。
            // 此操作需要对 SAS 的读取权限。
            CustomerEntity customerRead = null;
            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>(customer.PartitionKey, customer.RowKey);
                TableResult result = await table.ExecuteAsync(retrieveOperation);
                customerRead = result.Result as CustomerEntity;
                if (customerRead != null)
                {
                    Console.WriteLine("\t{0}\t{1}\t{2}\t{3}", customerRead.PartitionKey, customerRead.RowKey, customerRead.Email, customerRead.PhoneNumber);
                }

                Console.WriteLine("SAS {0} 的读取操作成功", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("SAS {0} 的读取操作失败", sasUri);
                    Console.WriteLine("附加错误信息： " + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            // 删除操作：删除一个实体。
            try
            {
                if (customerRead != null)
                {
                    await SamplesUtils.DeleteEntityAsync(table, customerRead);
                }

                Console.WriteLine("SAS {0} 的删除操作成功", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("SAS {0} 的删除操作失败", sasUri);
                    Console.WriteLine("附加错误信息：" + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            Console.WriteLine();
        }
    }
}
