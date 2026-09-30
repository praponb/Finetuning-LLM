from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.total_quantity") > 1020)
    .select(F.col("s.id"), F.col("s.store_name"), F.col("t.total_quantity"))
    .orderBy(F.col("id").desc())
)
