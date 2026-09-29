from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
        F.min("start_date").alias("first_start_date"),
    )
    .filter(F.col("paused_count") < 78)
    .select("plan", "paused_count", "first_start_date")
)
