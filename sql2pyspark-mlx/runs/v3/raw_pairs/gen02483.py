from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([5, 6]))
    .groupBy(F.col("p.category"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .filter(F.col("inventory_count") >= 4)
)
