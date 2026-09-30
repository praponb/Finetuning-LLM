from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") <= "2024-05-01")
    .groupBy(
        "plan",
        "customer_id",
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
    )
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .orderBy(F.col("customer_id").desc(), "paused_monthly_fee", "plan", "start_month")
    .limit(10)
)
