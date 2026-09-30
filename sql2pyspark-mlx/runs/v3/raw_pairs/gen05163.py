from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() | (F.col("dept_id") == 34))
    .select("job_title", "full_name", "email")
    .orderBy("full_name", "job_title")
)
