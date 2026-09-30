from pyspark.sql import functions as F

result = df_customers.filter((F.col("age") >= 33) & F.col("segment").isNotNull())
