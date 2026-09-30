from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 14) & F.col("dept_id").isin([16, 37]) & F.month("hire_date").isin([3, 9, 10]))
    .select(
        "id",
        "is_remote",
        "salary",
        F.upper("job_title").alias("job_title_upper"),
        F.lower("job_title").alias("job_title_lower"),
    )
)
