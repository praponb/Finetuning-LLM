from pyspark.sql import functions as F

result = df_customers.select("signup_date", "is_active", "city")
