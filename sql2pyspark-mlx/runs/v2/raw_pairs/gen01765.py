from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() | (F.col("start_date") <= "2025-07-01"))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    recent_subscriptions
    .groupBy("plan", "status")
    .agg(F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"))
)

result = agg
