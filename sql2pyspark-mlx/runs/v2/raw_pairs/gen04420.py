from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("we.page").isin(["/cart", "/checkout"]))
    .select(
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("s.started_at"),
        F.col("c.email"),
    )
)
