from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(F.col("s.store_name"), F.col("i.store_id"))
    .orderBy("store_name")
)
