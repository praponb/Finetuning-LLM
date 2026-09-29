from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([2, 5]))
    .groupBy(F.col("s.region"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
    .filter(F.col("store_sale_count") >= 5)
)
