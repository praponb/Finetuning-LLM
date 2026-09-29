from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id").isin([4, 6, 9])).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.region").isin(["North", "South"]))
    .select(F.col("s.id"), F.col("s.region"), F.col("s.store_name"), F.col("s.city"))
    .orderBy(F.col("store_name").desc(), "city")
)
