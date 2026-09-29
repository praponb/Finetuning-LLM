from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("i.last_restock")).isin([4, 7, 8]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.rating"),
        F.round((F.col("s.rating") * 1.15), 2).alias("rating_scaled"),
    )
)
