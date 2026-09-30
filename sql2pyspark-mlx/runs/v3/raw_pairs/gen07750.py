from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.total_revenue") > 10000)
    .select(F.col("s.id"), F.col("s.region"), F.col("t.total_revenue"))
)
