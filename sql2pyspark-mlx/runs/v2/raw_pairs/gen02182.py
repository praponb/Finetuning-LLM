from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"))
    .filter(F.col("paused_count") >= 3)
)
