from pyspark.sql import functions as F

result = df_customers.filter((F.length("email") > 5) & (F.col("signup_date") < "2025-07-04"))
