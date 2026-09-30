from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.col("inventory.last_restock") < "2023-10-13")
    .groupBy(F.col("products.brand"))
    .agg(
        F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"),
        F.min(F.col("inventory.last_restock")).alias("min_last_restock"),
    )
)
