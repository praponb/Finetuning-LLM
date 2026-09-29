from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id") == 3)
    .groupBy(F.col("ss.store_id"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("store_id").desc())
    .limit(10)
)
