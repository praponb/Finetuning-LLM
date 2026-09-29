from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .filter(F.col("products.brand").isNotNull())
    .groupBy(F.col("products.brand"))
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_quantity") > 720)
)
