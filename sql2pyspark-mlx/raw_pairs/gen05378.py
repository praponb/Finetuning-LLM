from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("oi.order_id").isin([5, 19, 23]))
    .select(
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("p.category"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
