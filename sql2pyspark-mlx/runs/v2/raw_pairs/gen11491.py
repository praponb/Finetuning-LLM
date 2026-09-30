from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "inner")
    .filter(F.col("employees_by_department.l2_count") > 3)
    .select(
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("employees_by_department.l2_count"),
    )
    .orderBy("dept_name")
)
