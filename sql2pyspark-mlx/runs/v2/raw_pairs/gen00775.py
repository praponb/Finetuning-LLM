from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .select(
        F.col("employees.id"),
        F.col("employees.job_title"),
        F.col("employees.email"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
    )
)
