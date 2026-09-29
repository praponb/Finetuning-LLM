from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "right")
    .filter(F.col("inventory.product_id").isin([8, 10]))
    .groupBy(F.quarter(F.col("products.launch_date")).alias("launch_quarter"))
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
    .orderBy(F.col("launch_quarter").desc(), "max_quantity")
)
