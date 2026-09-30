from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Initech") & (F.col("cost") <= 220) & (F.col("brand") == "Globex"))
    .withColumn("product_name_slug", F.regexp_replace("product_name", " ", ""))
    .withColumn("brand_length", F.length("brand"))
)
