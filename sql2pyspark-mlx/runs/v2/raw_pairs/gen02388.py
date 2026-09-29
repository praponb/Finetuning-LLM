from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.status") == "EXPIRED")
    .groupBy(F.col("c.city"))
    .agg(
        F.round(F.stddev(F.col("s.monthly_fee")), 2).alias("stddev_monthly_fee"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
)
