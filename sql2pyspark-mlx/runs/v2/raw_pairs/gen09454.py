from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "right")
    .filter(F.col("employees.job_title") == "Director")
    .select(
        F.col("employees.hire_date"),
        F.col("employees.dept_id"),
        F.col("employees.email"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.upper(F.col("departments.dept_name")).alias("dept_name_upper"),
    )
    .orderBy(F.col("budget").desc(), "dept_name_upper")
)
