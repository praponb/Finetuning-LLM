from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.country"),
        F.col("s.browser"),
        F.col("t.max_duration_sec"),
        F.col("t.page_view_count"),
    )
)
