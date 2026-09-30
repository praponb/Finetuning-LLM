from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.last_restock") < "2025-09-01")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("i.id")).alias("inventory_count"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
