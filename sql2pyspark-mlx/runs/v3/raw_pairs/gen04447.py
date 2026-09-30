from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Director"]))
    .select("email", "is_remote", "job_title", F.length("level").alias("level_length"))
)
