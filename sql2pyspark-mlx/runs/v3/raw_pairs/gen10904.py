from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        F.substring("email", 1, 2).alias("email_prefix"),
    )
)
