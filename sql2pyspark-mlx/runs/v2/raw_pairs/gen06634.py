from pyspark.sql import functions as F

result = df_store_sales.filter(F.col("product_id") == 6)
