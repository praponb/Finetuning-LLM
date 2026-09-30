from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") > "2024-01-01") & F.col("customer_id").isin([10, 11, 34]) & (F.col("start_date") >= "2023-04-05"))
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .orderBy(F.col("avg_monthly_fee").desc())
)
