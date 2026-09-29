from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft") <= 6462).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("i.last_restock") <= "2023-05-11")
    .select(F.col("i.id"), F.col("i.store_id"))
    .orderBy(F.col("id").desc(), "store_id")
)
