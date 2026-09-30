from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Intern")
    .select("id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("hire_date") >= "2024-04-01")
        .select("id", "job_title")
    )
)
