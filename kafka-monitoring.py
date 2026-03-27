from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

# 1. Update the topic (Ensure this matches your Team ID)
topic = 'movielog10' 

start_http_server(8765)

# 2. Define Metrics
# We add '_total' to the name as per Prometheus naming conventions for counters
REQUEST_COUNT = Counter(
    'request_count_total', 'Total number of recommendation requests',
    ['http_status']
)

REQUEST_LATENCY = Histogram(
    'request_latency_seconds', 'Time taken for recommendation requests',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
)

def main():
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'], # Note: passed as a list
        auto_offset_reset='latest',
        group_id=topic,
        # Using api_version helps avoid connection issues on some Python versions
        api_version=(0, 10) 
    )

    print(f"Monitoring topic: {topic}...")

    for message in consumer:
        try:
            event = message.value.decode('utf-8')
            values = [v.strip() for v in event.split(',')]
            
            # Check if it's a recommendation request
            # Usually values[2] is the request type
            if len(values) > 2 and 'recommendation request' in values[2].lower():
                
                # 3. Clean Status Code Extraction
                # Log usually looks like: "status 200"
                raw_status = values[3].strip() 
                status_code = raw_status.replace("status", "").strip()
                
                # Increment counter with the label
                REQUEST_COUNT.labels(http_status=status_code).inc()

                # 4. Latency Extraction
                # Log usually looks like: "45ms"
                raw_latency = values[-1]
                # Remove "ms" and convert to float
                latency_ms = float(raw_latency.replace("ms", "").strip())
                
                # Observe in seconds (standard Prometheus unit)
                REQUEST_LATENCY.observe(latency_ms / 1000.0)
                
        except Exception as e:
            # Important: Don't let one bad message crash your monitor
            print(f"Error parsing message: {e}")

if __name__ == "__main__":
    main()