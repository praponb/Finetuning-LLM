from pyspark.sql import functions as F

result = df_employees.withColumn("new_salary", F.col("salary") * 1.1)
