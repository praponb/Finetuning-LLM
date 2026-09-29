from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("units").isNull())
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.region"), F.col("s.store_name"), F.col("ss.avg_units"))
)
