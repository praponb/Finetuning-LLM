from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 15) & F.col("hire_date").between("2023-03-30", "2024-10-01"))
    .select(
        "id",
        "level",
        "dept_id",
        "salary",
        F.concat_ws(",", "job_title", "level").alias("job_title_level_key"),
    )
    .orderBy("job_title_level_key")
)
