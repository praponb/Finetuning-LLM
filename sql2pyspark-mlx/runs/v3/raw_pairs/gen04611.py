from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 190)
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

subscriptions_stats = (
    valid_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = subscriptions_stats.select("start_month", "status_list")
