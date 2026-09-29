from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_store_sales.alias("store_sales"), (F.col("inventory.store_id") == F.col("store_sales.store_id")) & (F.col("inventory.product_id") == F.col("store_sales.product_id")), "left")
    .filter(F.col("store_sales.sale_date") >= "2023-08-20")
    .select(
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("store_sales.sale_date"),
        F.col("store_sales.units"),
        F.col("store_sales.store_id").alias("store_sale_store_id"),
    )
    .orderBy(F.col("store_sale_store_id").desc())
)
