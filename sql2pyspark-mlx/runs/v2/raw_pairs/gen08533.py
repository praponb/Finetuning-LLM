from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Designer"]) & (F.col("job_title") == "Manager"))
    .select("id", "email")
)
