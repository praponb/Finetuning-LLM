from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title").isNull())
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("ebd.last_hire_date"))
)
