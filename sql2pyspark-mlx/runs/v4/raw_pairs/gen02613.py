from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("products.stock_qty") >= 136)
    .groupBy(F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"))
    .agg(F.countDistinct(F.col("inventory.quantity")).alias("unique_stores"))
    .orderBy(F.col("launch_month").desc())
)
