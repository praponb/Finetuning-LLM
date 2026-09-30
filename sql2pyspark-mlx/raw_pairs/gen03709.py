from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "hire_date",
        "salary",
        F.concat_ws(",", "level", "email").alias("level_email_key"),
        F.concat_ws(",", "full_name", "job_title").alias("full_name_job_title_key"),
    )
)
