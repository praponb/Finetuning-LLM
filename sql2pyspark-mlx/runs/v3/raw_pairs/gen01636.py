from pyspark.sql import functions as F

result = df_departments.select("id", "location", "dept_name")
