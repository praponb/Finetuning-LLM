from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("products.id") == F.col("inventory.product_id"), "inner")
    .filter(F.col("products.launch_date") <= "2024-02-09")
    .groupBy(F.col("products.brand"))
    .agg(F.max(F.col("inventory.quantity")).alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
