from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)
