from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "job_title",
        "hire_date",
        F.lower("job_title").alias("job_title_lower"),
    )
)
