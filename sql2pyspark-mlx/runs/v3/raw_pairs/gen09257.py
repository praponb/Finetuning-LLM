from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .groupBy("status", "plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
    .orderBy("plan", "avg_monthly_fee")
)
