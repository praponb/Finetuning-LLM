from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.revenue").between(3500, 3600))
    .select(
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("store_sales.revenue"),
    )
)
