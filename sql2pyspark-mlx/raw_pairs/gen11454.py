from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.store_id") == 6)
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("p.category"),
        F.col("p.supplier_id"),
    )
    .orderBy("store_name")
)
