from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.avg_salary") >= 113000)
    .select(F.col("d.id"), F.col("d.budget"), F.col("ebd.avg_salary"))
    .orderBy(F.col("budget").desc())
)
