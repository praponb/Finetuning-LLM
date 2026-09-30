from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2023-12-01")
    .groupBy("customer_id", "status")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.round(((100 * F.sum(F.when(F.col("status") == "EXPIRED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_expired"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy("status")
)
