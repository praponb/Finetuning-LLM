from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("we.event_time") >= "2025-10-18") & F.col("we.session_id").isin([6, 27, 32]))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
        F.col("c.name"),
    )
)
