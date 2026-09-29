from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.length("product_name") < 12) & F.col("product_name").isNull())
    .withColumn("brand_length", F.length("brand"))
)
