from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.sale_date") < "2023-12-22")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 5)
    .orderBy("store_name")
)
