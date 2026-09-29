from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 6]))
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(F.col("s.city"), F.col("S.stddev_units"))
    .orderBy("city")
)
