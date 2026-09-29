from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.product_id"),
    )
    .orderBy(F.col("quantity").desc())
)
