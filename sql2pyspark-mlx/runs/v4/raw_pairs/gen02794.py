from pyspark.sql import functions as F

result = df_employees.withColumn("clean_job_title", F.trim("job_title"))
