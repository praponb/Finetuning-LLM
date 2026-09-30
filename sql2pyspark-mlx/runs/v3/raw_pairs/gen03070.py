from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .filter(F.col("orders.customer_id").isin([1, 21, 34]) & F.col("payments.order_id").isin([5, 39]))
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("payments.status"),
        F.col("orders.order_date"),
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
    )
)
