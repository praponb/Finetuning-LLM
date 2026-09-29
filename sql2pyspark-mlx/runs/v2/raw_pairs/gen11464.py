from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.col("s.store_name").like("%Flagshi%"))
    .select(F.col("s.id"), F.col("s.city"), F.col("s.store_name"))
    .orderBy(F.col("id").desc())
)
