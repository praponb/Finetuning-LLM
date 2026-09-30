from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .select(
        F.col("employees.manager_id"),
        F.col("employees.salary"),
        F.col("departments.budget"),
    )
)
