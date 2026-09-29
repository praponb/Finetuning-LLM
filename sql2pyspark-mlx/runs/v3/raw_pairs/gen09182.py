from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("dept_id").isin([9, 18, 36])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("departments.location"),
    )
    .orderBy("location")
)
