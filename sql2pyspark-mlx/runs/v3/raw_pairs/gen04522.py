from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("employees.is_remote"))
    .select(
        F.col("employees.id"),
        F.col("employees.email"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
)
