from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.month(F.col("employees.hire_date")).isin([3, 5, 10]))
    .groupBy(F.col("departments.id"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("employees.level")))).alias("level_values"),
    )
    .filter(F.col("employee_count") > 3)
)
