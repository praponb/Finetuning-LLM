from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([1, 3, 4, 5]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.city"),
        F.col("p.supplier_id"),
        F.upper(F.trim(F.col("p.product_name"))).alias("product_name_normalized"),
    )
    .orderBy("city", "id")
)
