from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("salary") >= 115000).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .filter(F.col("departments.budget") > 3655000)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
    .orderBy(F.col("dept_name").desc())
)
