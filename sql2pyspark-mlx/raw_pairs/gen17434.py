from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
)
