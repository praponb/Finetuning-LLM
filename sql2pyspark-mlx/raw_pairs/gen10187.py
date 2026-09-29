from pyspark.sql import functions as F

result = df_products.agg(F.count("cost").alias("cost_count"))
