from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.product_id").isin([1, 2, 5, 7]))
    .groupBy(F.col("ss.product_id"), F.col("ss.store_id"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
)
