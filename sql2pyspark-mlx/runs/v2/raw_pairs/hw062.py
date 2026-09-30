from pyspark.sql import functions as F

result = df_customers.select("id", F.date_add("signup_date", 30).alias("trial_end"))
