from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") >= "2024-05-23") & (F.col("end_date") > "2023-10-01"))
    .groupBy("status", "customer_id")
    .agg(
        F.count("*").alias("row_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
    .orderBy("row_count")
)
