from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.city"),
        F.round((F.col("s.rating") * 0.85), 2).alias("adjusted_rating"),
    )
    .orderBy(F.col("last_restock").desc(), "id")
    .limit(5)
)
