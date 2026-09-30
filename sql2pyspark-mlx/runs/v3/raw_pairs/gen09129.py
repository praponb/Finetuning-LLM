from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17400, "high").when(F.col("c.credit_limit") >= 5900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
        F.sum(F.when(F.col("s.status") == "EXPIRED", F.col("s.monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
)
