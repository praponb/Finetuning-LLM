from pyspark.sql import functions as F

result = df_store_sales.filter((F.col("units") <= 26) & (F.col("units") < 13))
