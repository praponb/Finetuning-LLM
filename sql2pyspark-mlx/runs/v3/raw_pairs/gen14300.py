from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([32, 37, 38]) & (F.col("monthly_fee") < 80))
    .groupBy("plan", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") < 2)
    .drop("monthly_fee_count")
    .orderBy(F.col("first_end_date").desc())
)
