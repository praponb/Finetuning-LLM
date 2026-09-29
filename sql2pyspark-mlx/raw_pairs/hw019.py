from pyspark.sql import functions as F

result = df_customers.select("id", F.col("signup_date").cast("string").alias("signup_str"))
