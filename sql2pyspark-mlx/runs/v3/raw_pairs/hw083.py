from pyspark.sql import functions as F

result = df_customers.orderBy(F.col("credit_limit").desc_nulls_last())
