from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.to_date(F.col("s.started_at")) >= "2024-01-22")
    .select(
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("event_time").desc(), "started_at")
)
