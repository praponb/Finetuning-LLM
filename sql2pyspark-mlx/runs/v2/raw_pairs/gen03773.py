from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        "customer_id",
        F.when(F.col("monthly_fee") >= 188, "high").when(F.col("monthly_fee") >= 81, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
    )
    .filter(F.col("avg_monthly_fee") < 150)
)
