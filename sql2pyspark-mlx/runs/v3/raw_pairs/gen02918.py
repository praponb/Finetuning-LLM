from pyspark.sql import functions as F

result = (
    df_employees
    .select("salary", "job_title", "hire_date", F.upper("level").alias("level_upper"))
)
