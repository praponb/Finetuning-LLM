from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.budget").between(3941000, 4016000))
    .select(
        F.col("employees.manager_id"),
        F.col("employees.salary"),
        F.col("employees.dept_id"),
        F.col("departments.location"),
        F.col("departments.budget"),
    )
    .orderBy(F.col("budget").desc())
)
