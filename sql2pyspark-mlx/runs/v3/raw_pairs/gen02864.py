from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Engineer")
    .select(
        "id",
        "manager_id",
        "job_title",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
