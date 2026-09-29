from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.manager_id").isNull() & F.col("departments.dept_name").isin(["Marketing", "Engineering"]))
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("employees.job_title"),
        F.lower(F.col("departments.dept_name")).alias("dept_name_lower"),
    )
)
