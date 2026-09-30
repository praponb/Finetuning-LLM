from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.count("*").alias("subscription_count"),
        F.max("end_date").alias("latest_end_date"),
    )
    .orderBy("subscription_count", "min_start_date")
)
