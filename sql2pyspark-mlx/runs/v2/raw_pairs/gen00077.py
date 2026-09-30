from pyspark.sql import functions as F

result = df_customers.filter((F.col("city") != "New York") & (F.col("credit_limit") > 4500))
