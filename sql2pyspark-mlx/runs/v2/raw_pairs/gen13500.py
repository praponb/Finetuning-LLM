from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
    .orderBy(F.col("first_last_restock").desc(), "brand")
    .limit(10)
)
