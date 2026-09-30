from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull() & (F.lower(F.trim("product_name")) == "running shoes"))
    .withColumn("brand_category_key", F.concat_ws(",", "brand", "category"))
    .withColumn("category_prefix", F.substring("category", 1, 3))
)
