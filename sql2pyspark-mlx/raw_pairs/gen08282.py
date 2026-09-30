from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.min_duration_sec") > 240)
    .select(
        F.col("s.device"),
        F.col("ss.min_duration_sec"),
        F.col("ss.first_event_time"),
    )
    .orderBy("first_event_time")
)
