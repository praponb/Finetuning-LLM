from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Analyst")
    .select("hire_date", "job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title") == "Engineer")
        .select("hire_date", "job_title")
    )
)
