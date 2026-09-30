from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .select(
        F.col("employees.id"),
        F.col("employees.salary"),
        F.col("employees.manager_id"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
    )
    .orderBy(F.col("manager_id").desc())
)
