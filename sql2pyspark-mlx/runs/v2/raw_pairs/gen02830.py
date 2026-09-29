from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("plan") != "standard") & ((F.col("monthly_fee") >= 210) | (F.col("monthly_fee") >= 160)))
    .withColumn("start_date_plus_months", F.add_months("start_date", 6))
)

summary = (
    filtered_subscriptions
    .groupBy("status")
    .agg(
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .select("status", "stddev_monthly_fee", "total_count")
    .orderBy("stddev_monthly_fee", "status")
    .limit(20)
)
