using System;
using System.Collections.Generic;
using Xunit;
using Shouldly;
using System.Linq;
using EventStore.ClientAPI;
using System.Net;
using Newtonsoft.Json;
using EventStore.ClientAPI.Messages;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;


namespace PumpGetEventStore
{
    public class Program
    {
        static void Main(string[] args)
        {
            const int numberOfLocations = 100000;
            Console.WriteLine("Starting...");
            var address = IPAddress.Parse("172.17.8.101");
            var connection = EventStoreConnection.Create(new IPEndPoint(address, 1113));

            Console.WriteLine("sending {0} locations to the event store", numberOfLocations);
            Stopwatch watch = new Stopwatch();
            watch.Start();
            AppendLocations(connection, numberOfLocations).Wait();
            watch.Stop();
            double tps = (double)numberOfLocations / watch.Elapsed.TotalSeconds;
            Console.WriteLine("finished sending {0} locations, at rate of {1} tps", numberOfLocations, tps);
        }

        static Task AppendLocations(IEventStoreConnection connection, int howMany)
        {

            return Task.WhenAll(
                from location in Seed.Locations(howMany)
                let eventData = JsonConvert.SerializeObject(location).AsJson()
                select connection.AppendToStreamAsync("location", ExpectedVersion.Any, eventData));
        }
    }


    public static class Extensions
    {
        public static EventData AsJson(this object value)
        {
            if (value == null) throw new ArgumentNullException("value");

            var json = JsonConvert.SerializeObject(value);
            var data = Encoding.UTF8.GetBytes(json);
            var eventName = value.GetType().Name;

            return new EventData(Guid.NewGuid(), eventName, true, data, new byte[] { });
        }

        public static T ParseJson<T>(this RecordedEvent data)
        {
            if (data == null) throw new ArgumentNullException("data");

            var value = Encoding.UTF8.GetString(data.Data);

            return JsonConvert.DeserializeObject<T>(value);
        }
    }

   
    public class Location
    {
        public Guid Id { get; set; }
        public double Latitude { get; set; }
        public double Longitude { get; set; }
        public DateTime CreatedAt { get; set; }

    }

 
 

    public class Seed {
        public static List<Location> Locations(int howMany)
        {
            Random random = new Random();
            DateTime start = new DateTime(2014, 7, 1).ToUniversalTime();
            List<Location> list = new List<Location>();
            for (int i = 0; i < howMany;i++ )
            {
                Location location = new Location
                {
                    Id = Guid.NewGuid(),
                    Latitude = (random.NextDouble() * 2 - 1) * 90,
                    Longitude = (random.NextDouble() * 2 - 1) * 180,
                    CreatedAt = start 
                };
                list.Add(location);
                start = start.AddSeconds(1);
            }
            return list;
        }
    }

    public class Tests
    {
        [Fact]
        public void SeededLocations_ShouldNotReturn_AnEmptyList()
        {
            List<Location> locations = Seed.Locations(1000000);
            locations.ShouldNotBeEmpty();
        }

        [Fact]
        public void TheFirstSeededLocation_ShouldContain_ALatitudeALongitudeAndAGuid()
        {
            List<Location> locations = Seed.Locations(1000000);
            locations[0].Latitude.ShouldNotBe(0.0);
            locations[0].Longitude.ShouldNotBe(0.0);
        }

        [Fact]
        public void PrintLocations()
        {
            List<Location> locations = Seed.Locations(1000000);
            var sample = locations.Take(10);

            foreach (var location in sample)
            {
                Console.WriteLine("Id:{0}, Location ({1},{2}), CreateAt: {3}", location.Id, location.Longitude, location.Latitude, location.CreatedAt);
            }

        }
    }
}
