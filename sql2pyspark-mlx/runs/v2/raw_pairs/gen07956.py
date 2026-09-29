from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") <= 11943) | F.col("i.store_id").isin([5, 6]))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("s.sqft"))
)
