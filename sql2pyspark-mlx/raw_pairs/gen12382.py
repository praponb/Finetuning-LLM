from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 210) & (F.col("start_date") >= "2024-01-01"))
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(F.count("*").alias("subscription_count"))
    .filter(F.col("subscription_count") > 3)
    .orderBy(F.col("end_month").desc())
)
