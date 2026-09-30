from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-07-01", "2025-04-01") & (F.col("monthly_fee") >= 180))
    .groupBy("plan")
    .agg(F.avg("monthly_fee").alias("avg_monthly_fee"))
    .orderBy("avg_monthly_fee")
)
