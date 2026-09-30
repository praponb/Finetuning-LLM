from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.product_id").isin([10, 40]) & F.col("o.status").isin(["PENDING", "SHIPPED"]))
    .select(
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
)
