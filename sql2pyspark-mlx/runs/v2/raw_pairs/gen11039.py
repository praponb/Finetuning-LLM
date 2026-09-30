from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.shipping_fee") < 34)
    .select(
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.col("orders.order_date"),
    )
)
