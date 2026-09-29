from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("employees.manager_id").isin([4, 40]) & F.col("employees.is_remote"))
    .select(
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("employees.hire_date"),
        F.col("employees.level"),
        F.col("employees.full_name"),
    )
)
