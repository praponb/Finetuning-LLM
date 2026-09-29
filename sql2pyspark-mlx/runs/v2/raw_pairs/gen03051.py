from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.location") == "Chiang Mai")
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("employees.email"),
        F.col("employees.job_title"),
        F.col("employees.hire_date"),
    )
    .orderBy(F.col("job_title").desc(), "dept_name")
)
