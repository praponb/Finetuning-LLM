from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "eco") & F.col("p.supplier_id").isin([18, 27, 34, 40]))
    .select(
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
