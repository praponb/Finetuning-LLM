from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.shipping_fee").between(35, 36))
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("payments.paid_at"),
        F.col("payments.method"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
    )
)
