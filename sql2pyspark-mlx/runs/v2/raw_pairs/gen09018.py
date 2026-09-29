from pyspark.sql import functions as F

result = df_departments.select("location", "dept_name", "budget")
