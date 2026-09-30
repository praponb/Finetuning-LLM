from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter(F.col("customer_id").isin([16, 27]) | F.col("monthly_fee").between(90, 240))
    .withColumn("end_date_minus_6d", F.date_sub("end_date", 6))
)

plan_summary = (
    src
    .groupBy("plan")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    plan_summary
    .filter(F.col("avg_monthly_fee") < 70)
    .orderBy("pct_paused", "plan")
    .limit(5)
)
