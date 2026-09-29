from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.budget").isNotNull() & (F.year(F.col("employees.hire_date")) == 2024))
    .select(
        F.col("employees.is_remote"),
        F.col("employees.job_title"),
        F.col("employees.manager_id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
    )
)
