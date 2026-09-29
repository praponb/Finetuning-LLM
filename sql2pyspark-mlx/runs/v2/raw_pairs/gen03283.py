from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "level",
        "is_remote",
        "hire_date",
        F.concat_ws(",", "full_name", "job_title").alias("full_name_job_title_key"),
    )
)
