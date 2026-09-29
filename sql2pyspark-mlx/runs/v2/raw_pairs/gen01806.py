from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.quantity") != 130)
    .groupBy(F.col("store_sales.store_id"))
    .agg(
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
        F.count("*").alias("total_count"),
    )
)
