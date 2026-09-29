from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.row_count") >= 7)
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.row_count"))
)
