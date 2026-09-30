from pyspark.sql import functions as F

result = df_products.select("id", "category", "price", F.explode("tags").alias("tag"))
