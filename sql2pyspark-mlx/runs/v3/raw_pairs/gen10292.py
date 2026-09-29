from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.age"),
    )
)
