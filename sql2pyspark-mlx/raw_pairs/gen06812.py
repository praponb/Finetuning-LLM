from pyspark.sql import functions as F

valid_subscriptions = df_subscriptions.filter(F.col("status") == "PAUSED")

monthly_fee_bucket_summary = (
    valid_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 232, "high").when(F.col("monthly_fee") >= 121, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    monthly_fee_bucket_summary
    .select("monthly_fee_bucket", "unique_customers", "max_monthly_fee")
    .orderBy("monthly_fee_bucket")
)
