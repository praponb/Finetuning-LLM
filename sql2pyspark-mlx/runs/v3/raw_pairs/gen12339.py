from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.sort_array(F.collect_set(F.col("employees.level"))).alias("level_list"),
    )
    .orderBy(F.col("id").desc())
)
