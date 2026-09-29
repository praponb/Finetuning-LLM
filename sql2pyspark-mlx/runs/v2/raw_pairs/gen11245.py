from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type") != "click")
    .groupBy("session_id")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") > 360)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("t.min_duration_sec"),
    )
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
    .limit(1)
)
