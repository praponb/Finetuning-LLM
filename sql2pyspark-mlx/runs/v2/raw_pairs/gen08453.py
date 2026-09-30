from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select("id", "dept_id", "salary", F.trim("job_title").alias("clean_job_title"))
)
