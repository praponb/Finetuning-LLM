from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 138000)
    .select("id", "is_remote", "job_title")
)
