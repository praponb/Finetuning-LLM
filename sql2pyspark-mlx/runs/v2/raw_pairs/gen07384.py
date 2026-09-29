from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter((F.col("payments.status") == "FAILED") & F.col("orders.discount").isNotNull())
    .select(
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
        F.col("payments.amount"),
    )
)
