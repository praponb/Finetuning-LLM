from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        F.concat("job_title", F.lit(" - "), "level").alias("job_title_level"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
