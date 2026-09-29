from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.location"), F.col("ebd.intern_count"))
)
