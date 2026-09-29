from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_inventory.alias("inventory"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .filter(F.col("inventory.product_id").isin([1, 3, 6]))
    .groupBy(F.col("products.id"), F.col("products.category"))
    .agg(F.count(F.col("inventory.id")).alias("inventory_count"))
    .orderBy("id", F.col("inventory_count").desc(), "category")
    .limit(50)
)
