from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 4)
    .orderBy(F.col("quantity_count").desc(), F.col("product_id").desc())
)
