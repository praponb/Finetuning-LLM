from pyspark.sql import functions as F

result = df_employees.withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
