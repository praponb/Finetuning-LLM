from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.count("*").alias("inventory_count"))
    .orderBy(F.col("region").desc(), F.col("inventory_count").desc())
)
