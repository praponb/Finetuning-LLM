from pyspark.sql import functions as F

result = df_products.select("price", F.lower("product_name").alias("product_name_lower"))
