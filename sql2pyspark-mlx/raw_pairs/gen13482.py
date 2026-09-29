from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "premium"]) & (F.col("customer_id").isNull() | (F.col("end_date") > "2023-04-07")))
)

plan_summary = (
    valid_subscriptions
    .groupBy("plan")
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    plan_summary
    .filter(F.col("max_monthly_fee") < 70)
    .select("plan", "min_monthly_fee", "max_monthly_fee")
    .orderBy(F.col("min_monthly_fee").desc())
)
