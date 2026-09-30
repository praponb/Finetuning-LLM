from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("units").alias("units_count"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .filter(F.col("S.units_count") > 5)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"), F.col("S.units_count"))
)
