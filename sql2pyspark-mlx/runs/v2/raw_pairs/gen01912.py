from pyspark.sql import functions as F

result = df_products.select("id", "product_name", "category", F.explode("tags").alias("tag"))
