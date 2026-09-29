from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.year("last_restock") == 2025).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.sqft"))
    .orderBy(F.col("sqft").desc())
)
