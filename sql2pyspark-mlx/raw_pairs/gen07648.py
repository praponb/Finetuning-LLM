from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("plan").isin(["basic", "premium"]) & F.col("start_date").between("2024-02-01", "2025-01-01"))
    .groupBy("status", "plan")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)
