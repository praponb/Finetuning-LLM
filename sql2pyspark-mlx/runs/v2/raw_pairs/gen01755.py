from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("location") == "London").alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_anti")
    .filter(F.col("employees.email").like("%@outlook.com"))
    .select(
        F.col("employees.id"),
        F.col("employees.is_remote"),
        F.col("employees.level"),
    )
    .orderBy("level", F.col("id").desc())
)
