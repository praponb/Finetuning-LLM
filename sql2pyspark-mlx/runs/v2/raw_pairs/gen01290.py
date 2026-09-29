from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .filter(F.col("inventory.store_id").isin([3, 4, 5]))
    .groupBy(F.year(F.col("stores.opened_date")).alias("opened_year"))
    .agg(F.round(F.stddev(F.col("inventory.quantity")), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 70)
)
