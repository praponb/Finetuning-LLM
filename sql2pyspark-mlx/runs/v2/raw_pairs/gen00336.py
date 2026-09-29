from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-12-01")
    .select(
        "id",
        "job_title",
        "email",
        "level",
        F.concat_ws(",", "email", "full_name").alias("email_full_name_key"),
    )
)
