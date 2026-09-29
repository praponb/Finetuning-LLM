from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & (F.col("session_id") == 11))
    .select("id", "session_id", "event_type", "event_time")
    .orderBy(F.col("event_type").desc(), "event_time", "id")
    .limit(5)
)
