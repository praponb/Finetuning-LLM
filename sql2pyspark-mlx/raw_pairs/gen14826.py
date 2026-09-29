from pyspark.sql import functions as F

src = df_subscriptions.filter(F.year("start_date") >= 2025)

summary = (
    src
    .groupBy(F.month("end_date").alias("end_month"))
    .agg(F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"))
)

result = (
    summary
    .select("end_month", "stddev_monthly_fee")
    .orderBy(F.col("end_month").desc(), "stddev_monthly_fee")
    .limit(50)
)
