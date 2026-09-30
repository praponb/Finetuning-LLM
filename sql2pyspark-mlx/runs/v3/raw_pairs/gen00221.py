from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .select(
        F.col("departments.location"),
        F.col("employees.dept_id"),
        F.col("employees.email"),
    )
)
