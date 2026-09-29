from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("quantity") < 83).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.sqft") == 12044)
    .select(F.col("s.id"), F.col("s.store_name"))
)
