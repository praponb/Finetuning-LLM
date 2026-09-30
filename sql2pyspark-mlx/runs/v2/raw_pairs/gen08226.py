from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "outer")
    .select(
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("o.customer_id"),
        F.col("p.launch_date"),
    )
)
