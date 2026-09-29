from pyspark.sql import functions as F

result = df_employees.filter(F.col("dept_id").isin([11, 27, 32]) & (F.col("dept_id") == 14))
