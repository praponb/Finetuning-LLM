from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 16900, "high").when(F.col("c.credit_limit") >= 6000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"))
)
