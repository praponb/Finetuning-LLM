from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 20) & ((F.col("monthly_fee") >= 140) | (F.col("start_date") > "2023-03-01")))
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.max("end_date").alias("last_end_date"),
    )
    .filter(F.col("monthly_fee_range") >= 51)
    .orderBy("active_monthly_fee")
)
