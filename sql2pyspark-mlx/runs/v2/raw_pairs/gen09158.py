from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .select(
        F.col("inventory.id"),
        F.col("inventory.store_id"),
        F.col("products.launch_date"),
    )
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)
