from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("oi.product_id").isin([5, 13, 30]))
    .select(
        F.col("oi.order_id"),
        F.col("o.channel"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("supplier_id").desc())
)
