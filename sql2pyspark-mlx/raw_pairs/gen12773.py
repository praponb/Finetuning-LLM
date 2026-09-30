from pyspark.sql import functions as F

result = df_carts.select("id", F.explode("attributes").alias("k", "v"))
