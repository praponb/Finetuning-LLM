from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("oi.product_id").isin([1, 23, 25]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
