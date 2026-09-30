from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("cost").isNull()).alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_semi")
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.last_restock"),
    )
)
