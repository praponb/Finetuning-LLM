from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("o.store_id") == 16)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("o.status"),
    )
)
