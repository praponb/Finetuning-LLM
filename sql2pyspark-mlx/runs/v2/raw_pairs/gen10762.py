from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("payments.method") == "cash") & F.col("orders.store_id").isNotNull())
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.paid_at"),
        F.col("orders.discount"),
    )
)
