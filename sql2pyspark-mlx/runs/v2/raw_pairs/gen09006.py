from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 110)
)
