from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([30, 36, 37]) & (F.col("status") != "CANCELLED") & (F.col("end_date") > "2024-08-01"))
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)
