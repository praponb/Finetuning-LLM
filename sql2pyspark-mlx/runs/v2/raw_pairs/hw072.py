from pyspark.sql import functions as F

result = df_customers.select("id", F.concat_ws("-", "country", "city", "segment").alias("customer_key"))
