from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock") >= "2025-02-23")
    .select(
        F.col("s.city"),
        F.col("i.quantity"),
        F.length(F.col("s.store_name")).alias("store_name_length"),
    )
    .orderBy(F.col("city").desc())
)
