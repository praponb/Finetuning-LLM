from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("quantity") != 118)

store_id_summary = (
    recent_inventory
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    store_id_summary
    .select("store_id", "first_last_restock")
    .orderBy(F.col("store_id").desc())
    .limit(5)
)
