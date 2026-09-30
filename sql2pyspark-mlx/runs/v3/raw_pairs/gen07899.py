from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([1, 8, 11]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
)
