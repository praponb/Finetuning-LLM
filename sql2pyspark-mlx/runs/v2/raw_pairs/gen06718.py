from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .filter((F.col("payments.amount") >= 600) & (F.col("orders.status") == "COMPLETED"))
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("orders.status"),
        F.col("orders.shipping_fee"),
    )
)
