from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
        F.min(F.col("we.event_time")).alias("min_event_time"),
    )
)
