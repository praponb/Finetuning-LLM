from pyspark.sql import functions as F

result = df_employees.select("email", "manager_id", "hire_date")
