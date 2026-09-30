from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .filter(F.col("S.unique_products") >= 7)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("S.unique_products"))
    .orderBy(F.col("id").desc())
)
