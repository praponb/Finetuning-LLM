from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("s.started_at") >= "2024-06-01")
    .groupBy(F.col("c.country"))
    .agg(F.countDistinct(F.col("s.country")).alias("unique_countrys"))
)
