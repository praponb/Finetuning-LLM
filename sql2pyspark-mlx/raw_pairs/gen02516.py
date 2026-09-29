from pyspark.sql import functions as F

result = df_products.withColumn("price_int", F.col("price").cast("int"))
