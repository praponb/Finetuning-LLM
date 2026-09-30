from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .groupBy("store_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.cnt"))
    .orderBy(F.col("cnt").desc())
)
