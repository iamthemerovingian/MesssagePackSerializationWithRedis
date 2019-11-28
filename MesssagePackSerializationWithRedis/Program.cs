using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MesssagePackSerializationWithRedis
{
    class Program
    {
        static void Main(string[] args)
        {
            var redisCacheConnectionString = ConfigurationManager.ConnectionStrings["RedisCacheConnection"].ToString();
            var cacheConnection = ConnectionMultiplexer.Connect(redisCacheConnectionString);
            var serializer = new StackExchangeRedisExtensionsMessagePackSerializer();
            var cacheClient = new StackExchangeRedisCacheClient(serializer, cacheConnection.Configuration);

            var testModel = new TestModel
            {
                Now = DateTime.Now,
                UTCNow = DateTime.UtcNow
            };

            cacheClient.Add("testModel", testModel);

            var fromCache = cacheClient.Get<TestModel>("testModel");

            var isNowTimeSame = testModel.Now == fromCache.Now;
            var isNowTimeSamwWhenAdjusted = testModel.Now == fromCache.Now.AddHours(7);
            var isUtcNowTimeSame = testModel.UTCNow == fromCache.UTCNow;
        }

        public class TestModel
        {
            public DateTime Now { get; set; }
            public DateTime UTCNow { get; set; }
            
        }

        public class DurableDateTimeFormatter : IMessagePackFormatter<DateTime>
        {
            public DateTime Deserialize(byte[] bytes, int offset, IFormatterResolver formatterResolver, out int readSize)
            {
                if (MessagePackBinary.GetMessagePackType(bytes, offset) == MessagePackType.String)
                {
                    var str = MessagePackBinary.ReadString(bytes, offset, out readSize);
                    return DateTime.Parse(str);
                }
                else
                {
                    return MessagePackBinary.ReadDateTime(bytes, offset, out readSize);
                }
            }

            public int Serialize(ref byte[] bytes, int offset, DateTime value, IFormatterResolver formatterResolver)
            {
                if (value.Kind == DateTimeKind.Unspecified || value.Kind == DateTimeKind.Local)
                {
                    return MessagePackBinary.WriteDateTime(ref bytes, offset, value.AddHours(7));
                }

                return MessagePackBinary.WriteDateTime(ref bytes, offset, value);
            }
        }

        public class StackExchangeRedisExtensionsMessagePackSerializer : ISerializer
        {
            public StackExchangeRedisExtensionsMessagePackSerializer()
            {
                //MessagePackSerializer.SetDefaultResolver(MessagePack.Resolvers.ContractlessStandardResolverAllowPrivate.Instance);
                CompositeResolver.RegisterAndSetAsDefault(
                    new[] { new DurableDateTimeFormatter() },
                    new[] { MessagePack.Resolvers.ContractlessStandardResolverAllowPrivate.Instance });
            }
            public object Deserialize(byte[] serializedObject)
            {
                if (serializedObject == null)
                {
                    return default;
                }

                return MessagePackSerializer.Deserialize<object>(serializedObject);
            }

            public T Deserialize<T>(byte[] serializedObject)
            {
                if (serializedObject == null)
                {
                    return default;
                }

                return MessagePackSerializer.Deserialize<T>(serializedObject);
            }

            public async Task<object> DeserializeAsync(byte[] serializedObject)
            {
                return await Task.Run(() => Deserialize<object>(serializedObject));
            }

            public async Task<T> DeserializeAsync<T>(byte[] serializedObject)
            {
                return await Task.Run(() => Deserialize<T>(serializedObject));
            }

            public byte[] Serialize(object item)
            {
                if (item == null)
                {
                    return null;
                }

                return MessagePackSerializer.Serialize(item);
            }

            public async Task<byte[]> SerializeAsync(object item)
            {
                return await Task.Run(() => Serialize(item));
            }
        }
    }
}
