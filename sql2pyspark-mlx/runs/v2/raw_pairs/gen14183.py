from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2025-03-01")
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
    .orderBy(F.col("paused_monthly_fee").desc())
)
