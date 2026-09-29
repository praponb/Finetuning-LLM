from pyspark.sql import functions as F

result = df_store_sales.agg(F.max("revenue").alias("max_revenue"))
