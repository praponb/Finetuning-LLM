from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.price"),
        F.col("s.country"),
        F.col("s.city"),
        F.round((F.col("s.rating") * 1.1), 2).alias("rating_scaled"),
    )
    .orderBy(F.col("store_id").desc())
)
