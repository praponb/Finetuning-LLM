from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & (F.col("end_date") >= "2023-03-17"))
)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)

result = (
    subscriptions_stats
    .select("status", "avg_monthly_fee")
    .orderBy(F.col("status").desc())
    .limit(50)
)
