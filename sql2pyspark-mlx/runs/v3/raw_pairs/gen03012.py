from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-08-09", "2025-03-01"))
    .agg(
        F.max("end_date").alias("latest_end_date"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy(F.col("latest_end_date").desc())
)
