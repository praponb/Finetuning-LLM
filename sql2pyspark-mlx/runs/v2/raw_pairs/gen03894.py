from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2023-09-01"))
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.store_name"), F.col("ss.avg_quantity"))
    .orderBy("store_name")
)
