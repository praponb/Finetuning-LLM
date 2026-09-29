from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("num_store_sales"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.num_store_sales") > 5)
    .select(F.col("s.city"), F.col("s.opened_date"), F.col("t.num_store_sales"))
    .orderBy("city")
)
