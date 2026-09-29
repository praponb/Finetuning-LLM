from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("o.discount") <= 0.27)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("p.launch_date"),
    )
)
