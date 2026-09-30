from pyspark.sql import functions as F

result = df_customers.select("name", "credit_limit", "segment")
