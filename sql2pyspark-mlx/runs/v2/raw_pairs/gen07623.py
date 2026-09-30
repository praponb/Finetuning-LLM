from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNull() & F.col("manager_id").isin([4, 32, 37, 38]) & (F.col("hire_date") > "2025-06-01"))
    .select(
        "id",
        "job_title",
        "full_name",
        "is_remote",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)
