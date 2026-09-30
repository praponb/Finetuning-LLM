from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") != "L2")
    .select("id", "hire_date", "job_title")
)
