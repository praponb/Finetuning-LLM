from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.page_view_count") >= 38)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("t.page_view_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
