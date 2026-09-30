from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Manager") & F.col("dept_id").isin([4, 6]))
    .select(
        "id",
        "is_remote",
        "hire_date",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy(F.col("id").desc(), "hire_date")
    .limit(5)
)
