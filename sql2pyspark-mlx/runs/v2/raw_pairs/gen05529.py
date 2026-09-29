from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("amount") <= 700))
    .select(
        "id",
        "paid_at",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.concat_ws("/", "status", "method").alias("status_method_key"),
    )
    .orderBy("paid_at", "is_refunded")
)
