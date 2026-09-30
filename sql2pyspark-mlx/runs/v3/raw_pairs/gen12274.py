from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.supplier_id").isin([5, 25, 36, 38]))
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("p.category"),
    )
)
