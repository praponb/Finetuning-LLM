from pyspark.sql import functions as F

result = df_products.select("id", "brand", F.explode("tags").alias("tag_name"))
