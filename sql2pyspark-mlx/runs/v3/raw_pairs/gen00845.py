from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .select(
        F.col("employees.id"),
        F.col("employees.manager_id"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
    )
)
