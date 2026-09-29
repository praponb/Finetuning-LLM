from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("c.segment"),
    )
)
