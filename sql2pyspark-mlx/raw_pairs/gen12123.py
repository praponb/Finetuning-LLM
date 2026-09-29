from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue").isNotNull())
    .groupBy(F.col("ss.product_id"))
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy("product_id")
    .limit(5)
)
