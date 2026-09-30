from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .groupBy(F.col("products.brand"))
    .agg(F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") >= 60)
)
