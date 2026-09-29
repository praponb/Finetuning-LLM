from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.max_duration_sec") >= 140)
    .select(
        F.col("s.browser"),
        F.col("ss.page_view_count"),
        F.col("ss.max_duration_sec"),
    )
)
