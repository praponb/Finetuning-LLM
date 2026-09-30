from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.region"), F.col("t.unique_products"))
    .orderBy("region")
)
