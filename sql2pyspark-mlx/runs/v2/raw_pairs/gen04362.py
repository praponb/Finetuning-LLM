from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.store_id").isin([3, 5]))
    .groupBy(F.col("s.region"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy(F.col("region").desc(), "inventory_count")
)
