from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
    .orderBy("monthly_fee_count", "plan")
)
