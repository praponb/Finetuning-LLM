from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("revenue").between(1700, 3900))
    .groupBy("store_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.city"), F.col("t.revenue_range"))
)
