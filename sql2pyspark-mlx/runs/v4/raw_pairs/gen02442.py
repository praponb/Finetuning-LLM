from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "outer")
    .filter((F.col("employees.salary") >= 125000) & (F.col("employees.level") == "L5"))
    .select(
        F.col("employees.manager_id"),
        F.col("employees.is_remote"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
    )
)
