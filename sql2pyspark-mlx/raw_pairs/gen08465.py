from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.month(F.col("inventory.last_restock")).isin([1, 4, 6]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
        F.date_trunc("MONTH", F.col("store_sales.sale_date")).alias("sale_date_period"),
    )
)
