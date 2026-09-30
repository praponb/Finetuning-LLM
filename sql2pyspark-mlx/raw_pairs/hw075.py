from pyspark.sql import functions as F

result = df_products.select("id", F.upper("brand").alias("brand_upper"), F.lower("product_name").alias("name_lower"))
