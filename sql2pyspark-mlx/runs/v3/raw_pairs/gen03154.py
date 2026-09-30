from pyspark.sql import functions as F

result = df_departments.select("dept_name", F.upper("location").alias("location_upper"))
