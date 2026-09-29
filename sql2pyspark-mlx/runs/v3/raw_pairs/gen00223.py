from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.avg_salary") >= 35)
    .select(F.col("departments.location"), F.col("t.avg_salary"))
)
