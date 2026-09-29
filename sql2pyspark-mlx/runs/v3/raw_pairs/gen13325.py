from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("payments.status").isin(["SUCCESS", "FAILED"]) & F.col("orders.amount").isNotNull())
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("orders.status"),
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("payments.amount").cast("int").alias("amount_int"),
    )
)
