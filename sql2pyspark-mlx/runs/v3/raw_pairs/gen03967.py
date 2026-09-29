from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2023)
    .groupBy(
        F.when(F.year("end_date") < 2024, "before_2024").when(F.year("end_date") == 2024, "2024").otherwise("after_2024").alias("end_cohort"),
        "customer_id",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("pct_cancelled") >= 55)
    .select("end_cohort", "customer_id", "pct_cancelled", "stddev_monthly_fee")
    .orderBy(F.col("stddev_monthly_fee").desc(), "end_cohort", "customer_id")
    .limit(1)
)
