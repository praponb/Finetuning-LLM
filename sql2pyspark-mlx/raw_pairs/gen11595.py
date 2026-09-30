from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([1, 2, 5]))
    .groupBy(F.col("ss.store_id"))
    .agg(F.count("*").alias("inventory_count"))
    .filter(F.col("inventory_count") >= 2)
)
