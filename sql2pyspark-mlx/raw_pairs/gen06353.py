from pyspark.sql import functions as F

result = df_customers.withColumn("signup_date_month", F.month("signup_date"))
