from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select("id", "product_name", F.trim("product_name").alias("clean_product_name"))
)
