from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") >= 1200) & F.array_contains("tags", "bestseller"))
    .withColumn("product_name_prefix", F.substring("product_name", 1, 3))
)
