from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("start_date") <= "2025-07-03") & (F.col("monthly_fee") > 160)) | (F.col("monthly_fee") <= 90))
    .groupBy("customer_id", "plan")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)
