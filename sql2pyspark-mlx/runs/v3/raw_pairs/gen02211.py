from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("dept_id").isin([19, 20, 21, 29])).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .filter(F.col("departments.dept_name").isin(["Legal", "HR"]))
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
    )
)
