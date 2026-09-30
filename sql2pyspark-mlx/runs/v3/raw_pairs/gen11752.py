from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select("email", "full_name", F.lower("job_title").alias("job_title_lower"))
)
