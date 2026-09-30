from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.region"), F.col("ss.unique_stores"))
    .orderBy(F.col("region").desc(), "unique_stores")
)
