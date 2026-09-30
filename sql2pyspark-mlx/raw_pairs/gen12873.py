from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .groupBy(F.quarter(F.col("store_sales.sale_date")).alias("sale_quarter"))
    .agg(
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
        F.max(F.col("inventory.last_restock")).alias("latest_last_restock"),
    )
)
