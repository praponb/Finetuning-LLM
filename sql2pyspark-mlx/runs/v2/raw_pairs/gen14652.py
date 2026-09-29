from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .filter(F.col("inventory.quantity").isNotNull() | F.col("inventory.quantity").between(77, 98))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id").alias("store_sale_store_id"),
    )
)
