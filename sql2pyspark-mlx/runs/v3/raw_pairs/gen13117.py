from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") <= "2025-05-27") & F.col("full_name").isNull())
    .select(
        "id",
        "job_title",
        "hire_date",
        F.upper("job_title").alias("job_title_upper"),
    )
)
