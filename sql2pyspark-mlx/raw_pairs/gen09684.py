from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("opened_date") > "2023-05-01").alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_anti")
    .filter(F.col("inventory.quantity").between(99, 213))
    .select(F.col("inventory.id"), F.col("inventory.product_id"))
    .orderBy(F.col("product_id").desc())
)
