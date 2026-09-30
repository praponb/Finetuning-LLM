from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("inventory.store_id").isin([3, 5]))
    .select(
        F.col("inventory.last_restock"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
        F.col("store_sales.store_id"),
    )
    .orderBy(F.col("units").desc())
)
