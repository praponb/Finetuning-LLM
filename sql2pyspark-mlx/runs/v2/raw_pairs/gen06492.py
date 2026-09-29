from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("dept_name").isin(["Finance", "Sales", "Marketing"])).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.salary") >= 101000)
    .select(
        F.col("employees.id"),
        F.col("employees.full_name"),
        F.col("employees.salary"),
        F.col("employees.job_title"),
    )
)
