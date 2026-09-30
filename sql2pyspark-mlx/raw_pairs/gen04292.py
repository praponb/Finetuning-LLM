from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 21) & F.col("session_id").isin([2, 6, 10, 12]) & F.col("session_id").isin([1, 12, 26, 29]))
    .select("id", "event_time", "event_type", "session_id")
)
