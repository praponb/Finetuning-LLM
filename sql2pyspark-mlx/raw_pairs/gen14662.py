from pyspark.sql import functions as F

result = df_products.select("id", "category", F.ceil("price").alias("price_whole"))
