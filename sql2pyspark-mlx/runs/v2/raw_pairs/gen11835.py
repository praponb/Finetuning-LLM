from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.filter(F.col("last_restock").between("2024-02-29", "2025-01-07")).alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.sqft").between(7908, 10901))
    .select(F.col("s.id"), F.col("s.opened_date"))
)
