from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "inner")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("store_sales.units"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.revenue"),
        F.date_format(F.col("store_sales.sale_date"), "yyyy-MM").alias("sale_date_label"),
    )
)
