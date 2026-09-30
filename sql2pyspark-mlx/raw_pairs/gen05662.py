from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") >= 130)
)
