from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.col("s.city"),
    )
    .orderBy(F.col("unit_price").desc())
)
