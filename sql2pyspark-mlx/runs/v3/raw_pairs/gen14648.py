from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count("*").alias("subscription_count"),
    )
    .filter(F.col("subscription_count") < 7)
    .select("end_month", "avg_monthly_fee", "subscription_count")
    .orderBy(F.col("avg_monthly_fee").desc())
)
