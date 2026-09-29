from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([17, 19, 35]))
    .select("manager_id", "job_title", F.length("job_title").alias("job_title_length"))
)
