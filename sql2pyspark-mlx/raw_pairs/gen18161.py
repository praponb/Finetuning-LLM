from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("budget").between(2965000, 3048000)).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.level") == "L3")
    .select(F.col("employees.id"), F.col("employees.salary"), F.col("employees.email"))
    .orderBy(F.col("id").desc())
)
