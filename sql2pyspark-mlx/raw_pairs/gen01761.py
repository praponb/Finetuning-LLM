from pyspark.sql import functions as F

result = df_products.select("id", "stock_qty", F.array_join("tags", "|").alias("tags_text"))
