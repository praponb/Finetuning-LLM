from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter(((F.col("status") == "ACTIVE") & (F.col("monthly_fee") >= 80)) | (F.col("start_date") <= "2024-08-14"))
)

summary = (
    filtered_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    summary
    .select("customer_id", "monthly_fee_count")
    .orderBy(F.col("customer_id").desc())
)
