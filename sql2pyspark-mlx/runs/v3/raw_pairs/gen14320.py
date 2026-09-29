from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.budget"), F.col("ebd.latest_hire_date"))
)
