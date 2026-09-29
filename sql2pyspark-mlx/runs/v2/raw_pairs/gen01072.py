from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 70)
    .withColumn("end_date_month_end", F.last_day("end_date"))
)

plan_summary = (
    base
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
)

result = (
    plan_summary
    .filter(F.col("monthly_fee_range") >= 68)
    .select("plan", "monthly_fee_range", "total_monthly_fee")
)
