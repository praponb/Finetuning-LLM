from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter((F.col("store_sales.product_id") == 7) & F.col("store_sales.store_id").isin([1, 3, 4, 6]))
    .select(
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
        F.col("store_sales.product_id").alias("store_sale_product_id"),
    )
)
