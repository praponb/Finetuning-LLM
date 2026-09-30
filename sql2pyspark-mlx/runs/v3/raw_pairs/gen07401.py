from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "search") | F.col("page").isNotNull())
    .select("id", "duration_sec", "event_type", "session_id")
)
