from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("payments.paid_at") < "2024-01-10")
    .select(
        F.col("payments.status"),
        F.col("orders.order_date"),
        F.col("orders.shipping_fee"),
        F.col("orders.store_id"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
