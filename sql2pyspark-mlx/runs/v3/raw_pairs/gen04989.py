from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date") >= "2024-06-09")
    .groupBy("status", F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") < 520)
    .orderBy("start_month", "status")
    .limit(3)
)
