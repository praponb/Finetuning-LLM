from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.filter(F.col("brand") != "Stark").alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left_anti")
    .filter(F.col("inventory.product_id").isin([1, 4, 7, 9]))
    .select(
        F.col("inventory.id"),
        F.col("inventory.quantity"),
        F.col("inventory.product_id"),
        F.col("inventory.store_id"),
    )
)
