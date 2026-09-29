from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("*").alias("inventory_count"))
    .orderBy(F.col("product_id").desc())
)
