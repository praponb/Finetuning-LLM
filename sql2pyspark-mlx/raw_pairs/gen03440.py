from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 40) & F.col("status").isin(["CANCELLED", "EXPIRED"]) & (F.year("end_date") >= 2025))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("end_quarter").desc())
)
