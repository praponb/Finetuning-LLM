from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "dept_id",
        "salary",
        F.concat_ws(",", "level", "job_title").alias("level_job_title_key"),
    )
)
