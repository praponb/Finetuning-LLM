from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.name").like("%Ivy%"))
    .groupBy(
        F.col("c.city"),
        F.when(F.col("c.credit_limit") >= 16600, "high").when(F.col("c.credit_limit") >= 2600, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.count(F.when(F.col("s.device") == "mobile", 1)).alias("mobile_count"),
    )
)
