from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("id", "is_remote", F.lower("job_title").alias("job_title_lower"))
)
