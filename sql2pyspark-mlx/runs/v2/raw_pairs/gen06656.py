from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(F.col("s.id"), F.col("s.city"), F.col("t.min_sale_date"))
    .orderBy("id", F.col("city").desc())
)
