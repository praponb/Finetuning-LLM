from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date") <= "2025-02-09").alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("i.id"), F.col("i.last_restock"))
    .orderBy("last_restock", F.col("id").desc())
    .limit(10)
)
