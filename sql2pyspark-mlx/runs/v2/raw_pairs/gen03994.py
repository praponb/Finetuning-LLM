from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%AVI%"))
    .select(
        "id",
        "email",
        "is_remote",
        "dept_id",
        F.substring("level", 1, 3).alias("level_prefix"),
        F.length("job_title").alias("job_title_length"),
    )
)
