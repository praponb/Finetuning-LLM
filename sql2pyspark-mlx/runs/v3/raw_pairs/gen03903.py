from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("ssbs.total_revenue"))
    .orderBy(F.col("total_revenue").desc())
)
