from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "paid_at",
        "order_id",
        "amount",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
    )
)
