from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("start_date") > "2024-06-11")

summary = (
    valid_subscriptions
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"))
)

result = (
    summary
    .select("status", "active_count")
    .orderBy(F.col("status").desc(), "active_count")
)
