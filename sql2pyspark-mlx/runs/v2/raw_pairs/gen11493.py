from pyspark.sql import functions as F

result = df_employees.select("id", "manager_id", "job_title", "dept_id")
