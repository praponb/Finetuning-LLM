from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2023-11-01") & (F.col("end_date") < "2024-11-01"))
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"), "customer_id")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
    )
)
