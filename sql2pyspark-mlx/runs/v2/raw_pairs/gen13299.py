from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
    .orderBy("started_at")
)
