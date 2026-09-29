from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        F.concat_ws("-", "job_title", "email").alias("job_title_email_key"),
    )
)
