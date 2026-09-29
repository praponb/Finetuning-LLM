from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("product_id") == 1).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.region").isNull())
    .select(F.col("s.id"), F.col("s.region"), F.col("s.city"))
)
