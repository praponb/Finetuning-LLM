from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter((F.col("employees.hire_date") >= "2023-07-31") & F.col("employees.dept_id").isin([2, 25]))
    .select(
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("employees.dept_id"),
        F.col("employees.hire_date"),
    )
)
