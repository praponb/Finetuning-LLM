from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(F.col("store_sales.store_id"))
    .agg(F.avg(F.col("inventory.quantity")).alias("avg_quantity"))
    .orderBy(F.col("store_id").desc(), F.col("avg_quantity").desc())
)
