from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity") < 67).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.city").isin(["Berlin", "Chiang Mai"]))
    .select(F.col("s.id"), F.col("s.region"))
    .orderBy("region", F.col("id").desc())
)
