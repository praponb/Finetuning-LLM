from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .filter(F.year(F.col("inventory.last_restock")) == 2024)
    .groupBy(F.col("products.category"), F.col("products.brand"))
    .agg(
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
        F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") >= 77)
)
