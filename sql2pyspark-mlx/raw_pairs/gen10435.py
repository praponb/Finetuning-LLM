from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id").isin([1, 5, 9, 10]) & F.col("product_id").isin([6, 7])) | F.col("product_id").isin([2, 5, 7, 9]))
    .agg(F.max("quantity").alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
