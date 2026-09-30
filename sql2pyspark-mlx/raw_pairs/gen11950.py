from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "ACTIVE"]))
    .groupBy("status")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .filter(F.col("monthly_fee_count") >= 4)
    .select("status", "monthly_fee_count", "avg_monthly_fee")
)
