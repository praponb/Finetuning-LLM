from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_type") == "search")

web_events_stats = (
    base
    .groupBy("session_id")
    .agg(F.max("event_time").alias("last_event_time"))
)

result = (
    web_events_stats
    .select("session_id", "last_event_time")
    .orderBy(F.col("last_event_time").desc(), F.col("session_id").desc())
)
