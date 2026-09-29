from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.last_restock").between("2023-03-01", "2025-07-14") | F.col("i.store_id").isin([2, 6]))
    .select(F.col("s.store_name"), F.col("i.last_restock"))
    .orderBy("store_name")
)
