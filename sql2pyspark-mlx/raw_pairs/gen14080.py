from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("oi.order_id").isin([10, 13, 23, 25]) & F.array_contains(F.col("p.tags"), "bestseller"))
    .select(
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.rating"),
    )
)
