from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("full_name").like("Mi%"))
    .groupBy("dept_id")
    .agg(F.count("*").alias("employee_count"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.employee_count") >= 7)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ebd.employee_count"),
    )
    .orderBy(F.col("employee_count").desc())
)
