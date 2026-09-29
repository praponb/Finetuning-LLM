from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") < "2024-10-28").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.cost"), F.col("p.price"), F.col("p.launch_date"))
)
