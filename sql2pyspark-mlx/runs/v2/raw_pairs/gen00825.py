from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-10-20")
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.unique_products") >= 9)
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("ss.unique_products"))
)
