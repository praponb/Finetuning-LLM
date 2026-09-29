from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("oi.order_id").isin([6, 7]) | (F.col("oi.unit_price") > 170))
    .select(
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("p.brand"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("unit_price").desc())
)
