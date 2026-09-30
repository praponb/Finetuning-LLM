from pyspark.sql import functions as F

result = df_customers.select("id", F.regexp_extract("email", "@([a-z.]+)", 1).alias("domain"))
