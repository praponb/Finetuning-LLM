from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") > 4)
    .orderBy("store_name", "id")
    .limit(10)
)
