from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("full_name").like("%Jac%") & (F.col("hire_date") <= "2024-10-01"))
    .select("level", "job_title")
    .orderBy(F.col("job_title").desc())
)
