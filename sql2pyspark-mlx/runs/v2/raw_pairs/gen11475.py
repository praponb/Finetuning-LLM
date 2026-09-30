from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").isNull())
    .select(
        F.col("s.city"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
    .orderBy(F.col("store_id").desc())
)
