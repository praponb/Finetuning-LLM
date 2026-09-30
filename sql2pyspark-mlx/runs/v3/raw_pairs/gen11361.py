from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.region").isin(["East", "South"]))
    .select(F.col("s.id"), F.col("s.store_name"))
)
