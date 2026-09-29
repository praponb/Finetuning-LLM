from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.dept_name") == "Legal")
    .select(
        F.col("departments.budget"),
        F.col("employees.hire_date"),
        F.col("employees.email"),
        F.col("employees.dept_id"),
    )
    .orderBy("email")
)
