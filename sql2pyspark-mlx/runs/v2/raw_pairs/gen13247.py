from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "outer")
    .select(
        F.col("employees.id"),
        F.col("employees.salary"),
        F.col("employees.full_name"),
        F.col("employees.level"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.upper(F.col("departments.location")).alias("location_upper"),
    )
    .orderBy("location")
)
