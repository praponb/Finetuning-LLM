from pyspark.sql import functions as F

result = df_products.agg(F.min("launch_date").alias("min_launch_date"))
