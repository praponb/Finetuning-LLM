from pyspark.sql import functions as F

result = df_employees.filter(F.lower(F.trim("email")) == "alice@gmail.com")
