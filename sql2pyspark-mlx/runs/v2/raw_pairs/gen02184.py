from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([2, 30]) & ~F.col("job_title").isin(["Director", "Intern", "Analyst"]))
    .select(
        "id",
        "hire_date",
        F.length("job_title").alias("job_title_length"),
        F.upper("job_title").alias("job_title_upper"),
    )
)
