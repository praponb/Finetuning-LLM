from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("E"), F.col("d.id") == F.col("E.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("E.avg_salary"))
    .orderBy(F.col("budget").desc())
)
