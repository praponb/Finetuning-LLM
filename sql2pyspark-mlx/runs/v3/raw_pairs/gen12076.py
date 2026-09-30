from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 160) & F.col("customer_id").isin([17, 27, 29, 39]) & (F.col("plan") != "premium"))
    .groupBy(
        F.quarter("end_date").alias("end_quarter"),
        "status",
        F.when(F.col("monthly_fee") >= 260, "high").when(F.col("monthly_fee") >= 55, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .filter(F.col("standard_monthly_fee") < 77)
)
