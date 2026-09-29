from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") >= "2023-05-13").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.launch_date"))
    .orderBy("launch_date")
)
