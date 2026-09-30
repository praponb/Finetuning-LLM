from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .groupBy(F.col("departments.location"))
    .agg(F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") >= 53)
)
