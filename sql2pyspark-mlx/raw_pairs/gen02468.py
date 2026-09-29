from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy(F.col("id").desc())
)
