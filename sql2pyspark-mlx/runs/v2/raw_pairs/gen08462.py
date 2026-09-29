from pyspark.sql import functions as F

result = df_employees.filter(F.col("hire_date") < "2025-02-19")
