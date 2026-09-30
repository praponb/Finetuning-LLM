from pyspark.sql import functions as F

base = df_subscriptions.filter(F.col("plan") == "basic")

summary = (
    base
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
)

result = (
    summary
    .select("customer_id", "stddev_monthly_fee", "expired_count")
    .orderBy(F.col("customer_id").desc())
)
