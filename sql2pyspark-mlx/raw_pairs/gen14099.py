from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isin([1, 8]))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("o.order_date"),
        F.col("o.discount"),
    )
    .orderBy(F.col("discount").desc())
)
