from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.region"), F.col("ss.max_units"))
    .orderBy(F.col("region").desc())
)
