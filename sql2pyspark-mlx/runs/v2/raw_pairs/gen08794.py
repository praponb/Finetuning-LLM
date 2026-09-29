from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("region").isin(["Central", "West", "North"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.store_id").isin([4, 5]))
    .select(F.col("i.id"), F.col("i.last_restock"), F.col("i.store_id"))
)
