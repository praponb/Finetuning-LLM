from pyspark.sql import functions as F

result = df_products.select("id", "price", "product_name", F.explode("tags").alias("label"))
