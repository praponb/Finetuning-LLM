from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .select(F.col("s.city"), F.col("ssbs.latest_sale_date"))
    .orderBy("city")
)
