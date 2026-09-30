from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .groupBy(
        F.col("store_sales.store_id"),
        F.date_trunc("MONTH", F.col("store_sales.sale_date")).alias("sale_month"),
    )
    .agg(
        F.count(F.col("inventory.quantity")).alias("quantity_count"),
        F.countDistinct(F.col("inventory.quantity")).alias("unique_products"),
    )
)
