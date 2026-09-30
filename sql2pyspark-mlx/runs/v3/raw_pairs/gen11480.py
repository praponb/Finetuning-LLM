from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy("product_id", F.col("max_quantity").desc())
    .limit(1)
)
