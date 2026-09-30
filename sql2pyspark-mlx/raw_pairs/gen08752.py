from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 186, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
