from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.col("we.event_time") >= "2024-08-18") & (F.col("s.device") == "tablet"))
    .select(
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
)
