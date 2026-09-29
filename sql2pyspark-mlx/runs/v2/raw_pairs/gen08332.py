from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .filter(F.col("products.product_name").isNull())
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("inventory.product_id"),
        F.col("stores.sqft"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("sqft").desc(), F.col("id").desc())
)
