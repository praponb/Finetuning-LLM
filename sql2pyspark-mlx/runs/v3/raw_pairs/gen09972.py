from pyspark.sql import functions as F

result = df_employees.select("dept_id", "manager_id")
