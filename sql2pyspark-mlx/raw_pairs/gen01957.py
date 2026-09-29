from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([3, 7, 9]))
    .groupBy(F.date_trunc("MONTH", "end_date").alias("end_month"))
    .agg(
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
)
