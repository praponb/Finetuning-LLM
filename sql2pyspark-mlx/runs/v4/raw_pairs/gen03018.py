from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") >= "2025-10-25")
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") < 110)
    .orderBy(F.col("start_quarter").desc())
)
