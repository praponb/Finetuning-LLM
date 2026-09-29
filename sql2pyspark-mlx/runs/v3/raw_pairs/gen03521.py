from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "standard") & F.col("end_date").between("2023-11-02", "2025-06-07"))
    .groupBy(F.quarter("start_date").alias("start_quarter"), "status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
    .filter(F.col("monthly_fee_count") >= 1)
)
