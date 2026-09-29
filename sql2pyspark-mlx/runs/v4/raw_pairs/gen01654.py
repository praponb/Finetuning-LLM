from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Intern", "Analyst"]))
    .select("id", "level")
)
