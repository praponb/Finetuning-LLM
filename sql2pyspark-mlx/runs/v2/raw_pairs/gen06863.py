from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.col("customer_id").isin([5, 20, 21, 36]) & (F.col("customer_id") == 35)) | F.col("customer_id").isin([16, 20, 28, 30]))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

summary = (
    src
    .groupBy("plan")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = (
    summary
    .filter(F.col("stddev_monthly_fee") >= 21)
    .select("plan", "status_list", "stddev_monthly_fee")
    .orderBy("plan", F.col("stddev_monthly_fee").desc())
)
