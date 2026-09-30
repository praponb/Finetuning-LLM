from pyspark.sql import functions as F

result = df_employees.select("id", F.split("email", "@").alias("email_parts"))
