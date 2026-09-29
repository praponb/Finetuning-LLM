from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.month("hire_date").isin([3, 7, 9])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.dept_name"),
    )
    .orderBy("dept_name", "id")
    .limit(50)
)
