from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.customer_id").isNotNull())
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("s.started_at")).alias("min_started_at"))
)
