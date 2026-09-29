from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-10-01", "2025-07-01"))
    .groupBy(F.month("start_date").alias("start_month"), "status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") > 1000)
    .select("start_month", "status", "total_monthly_fee")
    .orderBy("start_month")
)
