from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .filter(F.col("ssbs.revenue_count") > 8)
    .select(F.col("s.id"), F.col("s.sqft"), F.col("ssbs.revenue_count"))
)
