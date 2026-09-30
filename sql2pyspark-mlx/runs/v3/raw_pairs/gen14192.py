from pyspark.sql import functions as F

result = df_customers.select("id", "segment", "credit_limit", "age")
