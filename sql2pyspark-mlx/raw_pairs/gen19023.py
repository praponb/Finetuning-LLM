from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.duration_sec"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("c.name"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("device").desc(), "started_at")
)
