from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.year("end_date") == 2024)

subscriptions_stats = (
    filtered_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"))
)

result = subscriptions_stats
