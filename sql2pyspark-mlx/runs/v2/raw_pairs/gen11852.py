from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "dept_id",
        "is_remote",
        "email",
        F.concat_ws("/", "job_title", "level").alias("job_title_level_key"),
    )
)
