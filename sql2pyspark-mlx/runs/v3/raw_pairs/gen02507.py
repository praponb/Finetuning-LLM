from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("event_time") >= "2025-01-30")

agg = (
    valid_web_events
    .groupBy("event_type", "session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)

result = (
    agg
    .filter(F.col("unique_event_types") > 9)
    .select("event_type", "session_id", "unique_event_types")
    .orderBy("event_type", "session_id")
    .limit(20)
)
