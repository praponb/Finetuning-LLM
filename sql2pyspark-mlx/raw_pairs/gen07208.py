from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("start_date").between("2024-04-16", "2025-10-01") & (F.col("monthly_fee") <= 90)) | (F.col("start_date") > "2023-11-30"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

summary = (
    recent_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
)

result = (
    summary
    .filter(F.col("avg_monthly_fee") < 160)
    .select("end_month", "avg_monthly_fee")
)
