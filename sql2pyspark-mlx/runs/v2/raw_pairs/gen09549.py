from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .filter(F.col("oi.unit_price").between(230, 300))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)
