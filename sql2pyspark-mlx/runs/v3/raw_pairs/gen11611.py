from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2024-06-01")
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 244, "high").when(F.col("monthly_fee") >= 89, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .orderBy(F.col("max_monthly_fee").desc(), "status", "monthly_fee_bucket")
    .limit(50)
)
