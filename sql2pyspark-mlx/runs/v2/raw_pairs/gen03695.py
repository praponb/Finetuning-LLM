from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Sports")
    .withColumn("product_name_title", F.initcap(F.trim("product_name")))
)
