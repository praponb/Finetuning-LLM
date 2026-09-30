from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([25, 26, 34, 40]))
    .select(
        "id",
        "full_name",
        "dept_id",
        F.concat_ws("/", "job_title", "full_name").alias("job_title_full_name_key"),
    )
)
