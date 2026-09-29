from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.country"), F.col("t.click_count"), F.col("t.avg_duration_sec"))
    .orderBy("country")
)
