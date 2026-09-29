from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("location").isNull()).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.job_title") == "Manager")
    .select(
        F.col("employees.id"),
        F.col("employees.hire_date"),
        F.col("employees.job_title"),
    )
)
