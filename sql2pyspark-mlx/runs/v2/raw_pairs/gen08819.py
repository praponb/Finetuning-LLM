from pyspark.sql import functions as F

result = df_products.select("id", "price", "brand", F.explode("tags").alias("label"))
