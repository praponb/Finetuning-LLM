from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.city"),
        F.col("s.region"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
