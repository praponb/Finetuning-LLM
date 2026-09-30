from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select("id", "email", F.trim("job_title").alias("clean_job_title"))
    .orderBy(F.col("id").desc())
)
