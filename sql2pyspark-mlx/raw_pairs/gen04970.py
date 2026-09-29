from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~((F.col("dept_id") == 31) & (F.col("job_title") != "Engineer") & (F.col("hire_date") > "2024-05-01")))
    .select("dept_id", "full_name", "manager_id", F.lower("email").alias("email_lower"))
)
