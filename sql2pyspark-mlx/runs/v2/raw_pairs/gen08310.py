from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.max("end_date").alias("last_end_date"),
    )
)
