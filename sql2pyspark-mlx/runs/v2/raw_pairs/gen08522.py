from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.year("last_restock") == 2023).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.launch_date"), F.col("p.category"))
    .orderBy(F.col("id").desc(), "launch_date")
)
