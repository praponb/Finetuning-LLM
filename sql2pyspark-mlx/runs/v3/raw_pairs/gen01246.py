from pyspark.sql import functions as F

result = df_employees.select("salary", F.upper(F.trim("email")).alias("email_normalized"))
