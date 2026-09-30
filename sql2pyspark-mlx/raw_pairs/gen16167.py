from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .select(
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("products.product_name"),
    )
)
