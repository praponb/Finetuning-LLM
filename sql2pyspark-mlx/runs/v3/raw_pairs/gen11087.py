from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.location") == "Berlin")
    .select(
        F.col("departments.location"),
        F.col("departments.dept_name"),
        F.col("employees.dept_id"),
        F.col("employees.email"),
        F.col("employees.full_name"),
    )
    .orderBy("location")
)
