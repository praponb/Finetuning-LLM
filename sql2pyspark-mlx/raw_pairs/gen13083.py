from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .groupBy("store_id")
    .agg((F.max("units") - F.min("units")).alias("units_range"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.units_range") > 76)
    .select(F.col("s.city"), F.col("t.units_range"))
    .orderBy("city")
)
