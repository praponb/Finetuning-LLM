from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("employees.full_name"),
        F.col("employees.email"),
        F.col("employees.is_remote"),
        F.initcap(F.trim(F.col("departments.dept_name"))).alias("dept_name_title"),
    )
)
