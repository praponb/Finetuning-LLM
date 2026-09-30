from pyspark.sql import functions as F

result = df_departments.select("id", "budget", "location", "dept_name")
