from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.length("full_name") > 10) & (F.col("hire_date").between("2023-05-01", "2024-07-01") | F.col("manager_id").isNotNull()))
    .select(
        "dept_id",
        "job_title",
        "manager_id",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy(F.col("manager_id").desc(), "job_title")
)
