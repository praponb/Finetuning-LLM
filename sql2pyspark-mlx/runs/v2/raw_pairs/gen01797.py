from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.store_id").isin([1, 5, 6]))
    .groupBy(F.col("store_sales.product_id"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .orderBy("row_count", "product_id")
    .limit(10)
)
