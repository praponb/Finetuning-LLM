from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("last_restock").between("2024-03-07", "2025-10-04")).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_semi")
    .filter(F.length(F.col("s.store_name")) < 13)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.store_name"), F.col("s.region"))
)
