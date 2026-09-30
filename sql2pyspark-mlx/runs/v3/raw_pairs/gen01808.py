from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "outer")
    .groupBy(F.col("products.category"))
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
)
