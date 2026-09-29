from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("i.store_id").isin([2, 3]) | F.col("i.product_id").isin([2, 5, 6]))
    .select(F.col("i.product_id"), F.col("s.region"), F.col("p.price"))
    .orderBy("product_id", F.col("region").desc())
)
