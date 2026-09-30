from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("p.category") == "Grocery") & (F.col("oi.quantity") < 7))
    .select(
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("o.store_id"),
        F.col("o.discount"),
    )
)
