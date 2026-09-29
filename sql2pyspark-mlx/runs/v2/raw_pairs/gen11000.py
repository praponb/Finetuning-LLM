from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity") != 63).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.store_name"))
    .orderBy(F.col("id").desc(), "store_name")
    .limit(5)
)
