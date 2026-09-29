from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("inventory.last_restock") <= "2023-06-01")
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("store_sales.revenue"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.product_id"),
        F.date_add(F.col("inventory.last_restock"), 41).alias("last_restock_plus_41d"),
    )
    .orderBy("product_id")
)
