from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.initcap(F.trim(F.col("p.brand"))).alias("brand_title"),
    )
    .orderBy(F.col("supplier_name").desc())
)
