from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-06-01", "2025-01-01"))
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 244, "high").when(F.col("monthly_fee") >= 121, "mid").otherwise("low").alias("monthly_fee_bucket"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy("customer_id")
)
