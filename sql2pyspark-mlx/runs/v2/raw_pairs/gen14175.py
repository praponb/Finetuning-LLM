from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .select(
        "amount",
        "order_id",
        "paid_at",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.round("amount", 0).alias("amount_rounded"),
    )
    .orderBy("amount_rounded")
)
