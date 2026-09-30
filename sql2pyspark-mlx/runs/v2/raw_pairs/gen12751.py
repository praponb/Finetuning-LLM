from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") >= "2023-05-14")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(F.count(F.col("i.id")).alias("inventory_count"))
    .orderBy("id")
)
