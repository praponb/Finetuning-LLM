from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.last_restock"),
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
        F.col("products.supplier_id"),
        F.col("products.category"),
        F.col("products.stock_qty"),
        F.concat(F.col("products.brand"), F.lit(" - "), F.col("products.category")).alias("brand_category"),
    )
    .orderBy(F.col("id").desc())
    .limit(3)
)
