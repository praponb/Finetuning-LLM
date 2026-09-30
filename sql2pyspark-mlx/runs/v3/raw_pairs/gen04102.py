from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.product_id").isin([2, 7, 9]) & F.col("inventory.product_id").isin([1, 4, 6]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
    )
    .orderBy("store_id")
)
