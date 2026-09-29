from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.max_duration_sec") > 240)
    .select(F.col("s.id"), F.col("s.device"), F.col("ss.max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc(), "id")
    .limit(5)
)
