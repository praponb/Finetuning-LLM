from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.store_id").isin([1, 2, 3]))
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("inventory.last_restock"),
        F.col("store_sales.revenue"),
        F.ceil(F.col("inventory.quantity")).alias("quantity_whole"),
    )
    .orderBy(F.col("quantity").desc())
)
