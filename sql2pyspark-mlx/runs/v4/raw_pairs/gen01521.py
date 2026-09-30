from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("budget") >= 3708000).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.salary") < 142000)
    .select(
        F.col("employees.id"),
        F.col("employees.job_title"),
        F.col("employees.salary"),
        F.col("employees.level"),
    )
)
