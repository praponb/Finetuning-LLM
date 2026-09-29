from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([4, 10, 12]) & (F.col("customer_id") == 15) & (F.col("monthly_fee") <= 90))
    .groupBy(
        "status",
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
        F.when(F.col("monthly_fee") >= 184, "high").when(F.col("monthly_fee") >= 120, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
    )
    .orderBy("monthly_fee_bucket", "end_cohort")
)
