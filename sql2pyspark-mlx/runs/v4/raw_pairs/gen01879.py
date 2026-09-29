from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-07-20", "2025-01-01"))
    .groupBy("customer_id")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_statuss"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .orderBy("customer_id")
)
