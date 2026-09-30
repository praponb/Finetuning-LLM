from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") | (F.col("job_title") != "Designer"))
    .select("id", "job_title", "dept_id")
)
