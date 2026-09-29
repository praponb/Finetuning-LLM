from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "right")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .groupBy(F.year(F.col("stores.opened_date")).alias("opened_year"))
    .agg(F.countDistinct(F.col("inventory.quantity")).alias("unique_products"))
    .filter(F.col("unique_products") >= 3)
    .orderBy("opened_year")
)
