from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-10-03", "2025-07-01"))
    .groupBy("status", "plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") < 9)
    .select("status", "plan", "monthly_fee_count")
)
