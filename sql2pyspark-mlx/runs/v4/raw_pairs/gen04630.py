from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 222, "high").when(F.col("monthly_fee") >= 62, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "status",
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)
