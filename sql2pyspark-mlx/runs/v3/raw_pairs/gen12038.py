from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("quantity") > 126)
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.avg_quantity") >= 200)
    .select(F.col("s.store_name"), F.col("ss.avg_quantity"))
    .orderBy(F.col("avg_quantity").desc(), "store_name")
)
