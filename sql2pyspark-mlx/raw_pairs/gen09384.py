from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(800, 1130) | F.month("launch_date").isin([2, 3, 5]))
    .withColumn("clean_product_name", F.trim("product_name"))
)
