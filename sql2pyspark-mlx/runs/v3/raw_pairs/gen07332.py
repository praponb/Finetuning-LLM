from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_time") >= "2023-08-11")
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.stddev_duration_sec") >= 34)
    .select(F.col("s.id"), F.col("s.device"), F.col("t.stddev_duration_sec"))
)
