from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .select(
        F.col("employees.id"),
        F.col("employees.full_name"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.when(F.col("employees.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)
