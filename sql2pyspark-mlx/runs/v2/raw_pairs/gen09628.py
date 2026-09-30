from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("start_date") >= "2023-05-01")

subscriptions_stats = (
    valid_subscriptions
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    subscriptions_stats
    .orderBy(F.col("customer_id").desc())
    .limit(5)
)
