from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.city") == "New York")
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("quantity").desc())
)
