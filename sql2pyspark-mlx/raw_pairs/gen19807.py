from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("city") == "Tokyo").alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.store_id").isin([1, 2, 4, 6]))
    .select(F.col("i.id"), F.col("i.quantity"), F.col("i.last_restock"))
)
