from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 22)
    .groupBy("plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") < 90)
    .select("plan", "monthly_fee_count", "min_monthly_fee")
    .orderBy(F.col("monthly_fee_count").desc(), "plan")
)
