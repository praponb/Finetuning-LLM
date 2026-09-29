from pyspark.sql import functions as F

result = df_products.select("id", "price", "supplier_id", F.size("tags").alias("tags_count"))
