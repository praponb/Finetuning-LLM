from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.store_id").isin([2, 3]) & (F.col("inventory.quantity") < 140))
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("store_sales.product_id"),
        F.add_months(F.col("inventory.last_restock"), 3).alias("last_restock_plus_months"),
    )
)
