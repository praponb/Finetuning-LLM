from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.min(F.col("we.event_time")).alias("first_event_time"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
    )
)
