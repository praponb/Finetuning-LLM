from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.supplier_id") == 21)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("s.rating"),
        F.round((F.col("s.rating") * 0.85), 2).alias("adjusted_rating"),
    )
    .orderBy(F.col("rating").desc())
)
