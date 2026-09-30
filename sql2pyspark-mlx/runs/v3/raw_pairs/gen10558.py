from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("quantity") != 109)
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.avg_quantity") > 180)
    .select(F.col("s.id"), F.col("s.store_name"), F.col("t.avg_quantity"))
)
