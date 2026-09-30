from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("email").like("%@outlook.com"))
    .select(
        "manager_id",
        "email",
        "is_remote",
        F.upper("job_title").alias("job_title_upper"),
    )
)
