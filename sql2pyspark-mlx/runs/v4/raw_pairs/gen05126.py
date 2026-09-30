from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-11-01") & (F.col("paid_at") >= "2025-05-01"))
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.max("amount").alias("max_amount"),
    )
)
