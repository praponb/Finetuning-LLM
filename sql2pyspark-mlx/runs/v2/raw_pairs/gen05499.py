from pyspark.sql import functions as F

result = df_customers.agg(F.sum("credit_limit").alias("total_credit_limit"))
