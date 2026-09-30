from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("order_id") == 11))
    .select(
        "id",
        "amount",
        "order_id",
        "status",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
)
