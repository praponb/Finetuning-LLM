from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(
        F.col("s.customer_id"),
        F.col("t.total_duration_sec"),
        F.col("t.pct_page_view"),
    )
    .orderBy(F.col("pct_page_view").desc())
)
