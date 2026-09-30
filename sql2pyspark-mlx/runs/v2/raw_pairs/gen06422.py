from pyspark.sql import functions as F

result = df_products.select("id", "brand", "price", F.explode("tags").alias("tag"))
