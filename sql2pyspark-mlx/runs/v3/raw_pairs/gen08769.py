from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("start_date") >= "2024-11-01") | F.col("status").isin(["CANCELLED", "EXPIRED"]))
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy("plan", "customer_id")
    .agg(
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
)

result = (
    subscriptions_stats
    .filter(F.col("min_monthly_fee") < 220)
    .select("plan", "max_monthly_fee", "min_monthly_fee")
    .orderBy("plan", F.col("min_monthly_fee").desc())
)
