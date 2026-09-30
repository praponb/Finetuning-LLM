from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.filter(F.col("amount").between(550, 1420)).alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "left_semi")
    .filter(F.year(F.col("orders.order_date")) == 2025)
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("orders.customer_id"),
    )
)
