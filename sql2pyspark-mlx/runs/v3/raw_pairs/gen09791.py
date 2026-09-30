from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.row_count") >= 9)
    .select(F.col("s.region"), F.col("t.row_count"))
    .orderBy(F.col("row_count").desc())
)
