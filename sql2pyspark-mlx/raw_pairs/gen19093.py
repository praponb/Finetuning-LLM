from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
