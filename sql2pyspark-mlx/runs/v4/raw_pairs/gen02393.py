from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("quantity").between(121, 220))
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("s.store_name"), F.col("t.avg_quantity"))
)
