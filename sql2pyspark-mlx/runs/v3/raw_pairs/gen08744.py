from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_count"),
    )
    .filter(F.col("active_count") >= 35)
    .drop("active_count")
    .orderBy(F.col("expired_count").desc())
)
