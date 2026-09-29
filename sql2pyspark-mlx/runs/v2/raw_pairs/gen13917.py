from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Ross%"))
    .select(
        "id",
        "email",
        "hire_date",
        "job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
