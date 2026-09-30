from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .orderBy(F.col("monthly_fee_count").desc())
)
