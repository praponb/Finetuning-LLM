from pyspark.sql import functions as F

result = df_store_sales.agg(F.min("sale_date").alias("first_sale_date"))
