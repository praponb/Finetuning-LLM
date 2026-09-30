from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("hire_date") <= "2024-06-01").alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("departments.budget"),
    )
)
