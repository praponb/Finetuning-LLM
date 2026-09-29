from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") >= "2024-10-01").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.product_name"), F.col("p.brand"))
)
