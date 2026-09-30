from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.month(F.col("store_sales.sale_date")).isin([4, 8, 11]))
    .groupBy(F.col("store_sales.product_id"), F.col("store_sales.store_id"))
    .agg(F.min(F.col("inventory.quantity")).alias("min_quantity"))
)
