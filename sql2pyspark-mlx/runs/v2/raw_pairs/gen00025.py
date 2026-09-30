from pyspark.sql import functions as F

result = df_products.select("id", "price", F.explode("tags").alias("label"))
