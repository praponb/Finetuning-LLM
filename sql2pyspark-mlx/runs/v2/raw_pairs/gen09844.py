from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.product_id").isNull() & (F.col("oi.quantity") > 3))
    .select(
        F.col("p.supplier_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("supplier_id").desc(), F.col("product_id").desc())
)
