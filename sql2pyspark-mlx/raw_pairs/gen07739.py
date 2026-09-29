from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.dept_name").isNull())
    .select(
        F.col("employees.id"),
        F.col("employees.job_title"),
        F.col("employees.is_remote"),
        F.col("employees.email"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
    )
    .orderBy("id")
)
