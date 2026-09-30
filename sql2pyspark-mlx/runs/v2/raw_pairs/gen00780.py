from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("ss.page_view_count"),
        F.col("ss.min_event_time"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("device").desc())
)
