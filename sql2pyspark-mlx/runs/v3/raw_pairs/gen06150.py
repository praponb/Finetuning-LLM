from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("s.supplier_name"),
    )
    .orderBy("unit_price", "id")
    .limit(1)
)
