from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") >= 60)
    .orderBy(F.col("last_restock_quarter").desc())
)
