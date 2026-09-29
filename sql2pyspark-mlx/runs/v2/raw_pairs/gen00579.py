from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.year("end_date") == 2024) & (F.col("status") != "ACTIVE") & (F.col("status") == "CANCELLED"))
)

subscriptions_stats = (
    recent_subscriptions
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
    )
)

result = subscriptions_stats.select("status", "expired_monthly_fee")
