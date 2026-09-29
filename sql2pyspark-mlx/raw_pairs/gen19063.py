from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .groupBy(F.col("p.brand"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") > 5)
    .orderBy("inventory_count", "brand")
    .limit(5)
)
