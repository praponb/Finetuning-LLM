from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.stddev_duration_sec") >= 59)
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("t.stddev_duration_sec"),
        F.col("t.latest_event_time"),
    )
)
