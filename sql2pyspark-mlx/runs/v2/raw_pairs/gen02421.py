from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 38]))
    .select("id", "page", "session_id", F.lower("event_type").alias("event_type_lower"))
    .orderBy("session_id", "id")
    .limit(5)
)
