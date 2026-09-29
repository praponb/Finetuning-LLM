from pyspark.sql import functions as F

result = df_products.select("id", "price", "category", F.explode("tags").alias("tag"))
