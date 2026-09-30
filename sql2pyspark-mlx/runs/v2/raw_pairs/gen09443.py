from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.email"),
    )
)
