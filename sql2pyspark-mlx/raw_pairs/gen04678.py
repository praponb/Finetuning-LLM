from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("salary") > 70000).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
)
