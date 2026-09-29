from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "salary",
        F.length("job_title").alias("job_title_length"),
    )
)
