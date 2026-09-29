from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("status") == "REFUNDED") & F.col("amount").isNotNull())
    .select(
        "id",
        "paid_at",
        "order_id",
        "amount",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.substring("method", 1, 4).alias("method_prefix"),
    )
    .orderBy("status_method_key")
)
