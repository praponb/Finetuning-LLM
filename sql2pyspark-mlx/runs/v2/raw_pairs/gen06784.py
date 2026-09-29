from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id") == 26)
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ebd.min_salary"),
        F.col("ebd.max_salary"),
    )
    .orderBy(F.col("budget").desc())
)
