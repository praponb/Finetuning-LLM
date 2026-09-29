from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.count("*").alias("row_count"),
        F.count("*").alias("subscription_count"),
    )
    .filter(F.col("row_count") > 1)
)
