from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("s.customer_id").isNull())
    .groupBy(F.col("s.device"), F.col("s.country"))
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"))
    .orderBy(F.col("country").desc())
)
