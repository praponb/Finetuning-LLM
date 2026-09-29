from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "salary",
        "is_remote",
        "manager_id",
        F.trim("job_title").alias("clean_job_title"),
    )
)
