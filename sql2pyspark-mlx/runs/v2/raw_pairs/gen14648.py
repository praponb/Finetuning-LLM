from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("employees.is_remote"),
        F.col("employees.job_title"),
        F.col("employees.manager_id"),
        F.col("employees.is_remote").cast("int").alias("is_remote_flag"),
    )
)
