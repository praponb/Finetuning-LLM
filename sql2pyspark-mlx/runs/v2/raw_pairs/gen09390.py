from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Manager", "Analyst"]))
    .select("id", "level", "job_title", "hire_date")
)
