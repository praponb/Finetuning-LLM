from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.device"),
        F.col("s.country"),
        F.col("c.is_active"),
        F.col("c.email"),
    )
)
