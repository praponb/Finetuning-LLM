from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "outer")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .groupBy(F.col("products.brand"))
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("total_quantity") > 1020)
    .orderBy("total_quantity", "stddev_quantity")
)
