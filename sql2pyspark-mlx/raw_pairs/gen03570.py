from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("plan").isin(["basic", "standard"]))

subscriptions_stats = (
    valid_subscriptions
    .groupBy("plan")
    .agg(
        F.max("start_date").alias("latest_start_date"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

result = subscriptions_stats.orderBy("plan")
