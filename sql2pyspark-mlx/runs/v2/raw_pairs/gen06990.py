from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter((F.col("employees.salary") < 65000) | (F.col("departments.dept_name") != "Engineering"))
    .select(
        F.col("employees.id"),
        F.col("employees.full_name"),
        F.col("employees.salary"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
)
