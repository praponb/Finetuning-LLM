from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isin([7, 16]) & F.col("customer_id").isNotNull())
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = subscriptions_stats
