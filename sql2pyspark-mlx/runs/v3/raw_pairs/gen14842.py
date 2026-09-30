from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.year("start_date") >= 2025)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy("plan", "customer_id")
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    subscriptions_stats
    .filter(F.col("min_monthly_fee") > 200)
    .orderBy(F.col("min_monthly_fee").desc(), "customer_id")
)
