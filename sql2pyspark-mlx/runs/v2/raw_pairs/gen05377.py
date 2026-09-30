from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "CANCELLED")
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("subscription_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("subscription_count") < 6)
    .select("customer_id", "subscription_count", "max_monthly_fee")
    .orderBy(F.col("max_monthly_fee").desc())
)
