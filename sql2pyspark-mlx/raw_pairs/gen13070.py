from pyspark.sql import functions as F

result = (
    df_employees
    .select("level", "job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title") != "Intern")
        .select("level", "job_title")
    )
)
