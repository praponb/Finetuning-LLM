from pyspark.sql import functions as F

result = df_store_sales.select("id", "revenue", "store_id", "units")
