from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(201, 208))
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
)
