from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 3)
    .withColumn("clean_job_title", F.trim("job_title"))
    .withColumn("salary_whole", F.ceil("salary"))
)
