from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull() & (F.col("o.store_id") == 11))
    .select(
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("o.discount"),
    )
)
