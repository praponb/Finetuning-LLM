from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("inventory.store_id").isin([1, 3]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
        F.col("store_sales.product_id").alias("store_sale_product_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
    )
    .orderBy("revenue", "quantity")
)
