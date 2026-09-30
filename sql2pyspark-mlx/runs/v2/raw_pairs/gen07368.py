from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.col("employees.email").like("%@gmail.com"))
    .select(
        F.col("departments.dept_name"),
        F.col("employees.job_title"),
        F.col("employees.dept_id"),
        F.col("employees.is_remote"),
    )
)
