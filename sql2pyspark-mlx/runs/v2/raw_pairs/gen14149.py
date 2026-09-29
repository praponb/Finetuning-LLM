from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("s.region").isin(["South", "East"]) & (F.col("i.product_id") == 4))
    .select(F.col("i.last_restock"), F.col("s.city"), F.col("s.store_name"))
    .orderBy("store_name", F.col("city").desc())
)
