from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([26, 37]) & ((F.col("status") == "FAILED") | (F.col("paid_at") >= "2025-04-01")))
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.min("amount").alias("min_amount"),
    )
)
