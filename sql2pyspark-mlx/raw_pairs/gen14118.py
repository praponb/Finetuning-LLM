from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .groupBy(F.col("departments.dept_name"))
    .agg(
        F.max(F.col("employees.hire_date")).alias("latest_hire_date"),
        F.round(F.stddev(F.col("employees.salary")), 2).alias("stddev_salary"),
    )
    .filter(F.col("stddev_salary") >= 25)
)
