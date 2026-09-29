from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("inventory.quantity").between(201, 230))
    .groupBy(F.quarter(F.col("stores.opened_date")).alias("opened_quarter"))
    .agg(
        F.round(F.avg(F.col("inventory.quantity")), 2).alias("avg_quantity"),
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 120)
)
