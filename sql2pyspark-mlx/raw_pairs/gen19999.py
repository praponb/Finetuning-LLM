from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.min_revenue") > 1300)
    .select(F.col("s.city"), F.col("ss.min_revenue"))
)
