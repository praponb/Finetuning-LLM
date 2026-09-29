from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy("brand")
    .limit(3)
)
