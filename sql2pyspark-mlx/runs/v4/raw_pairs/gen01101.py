from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.product_id").isin([8, 9]) | (F.col("store_sales.units") <= 22))
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.store_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.store_id").alias("store_sale_store_id"),
        F.col("store_sales.units"),
        F.datediff(F.current_date(), F.col("inventory.last_restock")).alias("days_since_last_restock"),
    )
    .orderBy(F.col("id").desc())
)
