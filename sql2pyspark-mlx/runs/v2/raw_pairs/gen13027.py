from pyspark.sql import functions as F

result = df_store_sales.filter(F.month("sale_date").isin([7, 9, 11]))
