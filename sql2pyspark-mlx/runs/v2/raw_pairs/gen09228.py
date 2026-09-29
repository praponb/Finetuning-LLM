from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("last_restock") > "2024-01-01")
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.stddev_quantity") > 66)
    .select(F.col("s.region"), F.col("ss.stddev_quantity"))
    .orderBy(F.col("region").desc())
)
