from pyspark.sql import functions as F

result = df_employees.select("job_title", "hire_date", "email")
