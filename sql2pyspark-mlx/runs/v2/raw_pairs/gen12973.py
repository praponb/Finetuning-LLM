from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") > "2023-08-07") & ~F.col("plan").isin(["standard", "enterprise", "premium"]))
)

plan_summary = (
    valid_subscriptions
    .groupBy("plan", F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0))) / F.count("*")), 1).alias("pct_enterprise"),
    )
)

result = (
    plan_summary
    .filter(F.col("monthly_fee_count") > 8)
    .orderBy(F.col("pct_enterprise").desc())
)
