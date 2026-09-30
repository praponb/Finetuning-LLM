from pyspark.sql import functions as F

result = df_customers.select("id", "signup_date", F.length("email").alias("email_length"))
