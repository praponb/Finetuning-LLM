from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-06-01", "2025-06-01") & F.col("monthly_fee").between(80, 100))
    .groupBy("status", "plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .orderBy("monthly_fee_count")
)
