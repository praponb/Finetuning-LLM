from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.hour(F.col("we.event_time")).between(10, 18))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("c.is_active"),
        F.col("c.email"),
    )
    .orderBy(F.col("device").desc(), "event_type")
)
