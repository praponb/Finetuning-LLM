from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.segment"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
    )
    .orderBy(F.col("id").desc(), "min_monthly_fee")
)
