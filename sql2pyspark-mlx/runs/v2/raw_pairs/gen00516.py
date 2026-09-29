from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select("level", "job_title")
    .distinct()
)
