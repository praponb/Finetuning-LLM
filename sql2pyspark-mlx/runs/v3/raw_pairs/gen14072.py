from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([15, 30]))
    .select(
        "id",
        "salary",
        F.floor("salary").alias("salary_whole"),
        F.lower("job_title").alias("job_title_lower"),
    )
)
