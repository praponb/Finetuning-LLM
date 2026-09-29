from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([14, 26, 36, 39]))
    .withColumn("clean_status", F.trim("status"))
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy(F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = subscriptions_stats
