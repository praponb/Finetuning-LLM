from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id") == 28)
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ebd.last_hire_date"),
        F.col("ebd.total_salary"),
    )
    .orderBy("last_hire_date", F.col("id").desc())
)
