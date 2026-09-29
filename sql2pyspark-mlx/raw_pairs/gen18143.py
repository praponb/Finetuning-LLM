from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") | (F.col("start_date") >= "2024-11-01"))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.stddev("days_since_start_date"), 2).alias("stddev_days_since_start_date"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .select("status", "stddev_days_since_start_date", "avg_monthly_fee")
)
