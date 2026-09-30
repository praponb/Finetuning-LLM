from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("start_date") >= "2025-04-01")

status_summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
)

result = status_summary.select("status", "stddev_monthly_fee", "standard_count")
