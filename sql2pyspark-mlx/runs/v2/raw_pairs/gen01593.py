from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.region"), F.col("s.city"), F.col("t.avg_units"))
)
