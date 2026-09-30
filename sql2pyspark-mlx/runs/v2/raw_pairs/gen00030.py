from pyspark.sql import functions as F

result = df_products.select("id", "product_name", F.explode("tags").alias("tag"))
