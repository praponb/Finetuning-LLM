from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("hire_date").between("2023-12-14", "2024-02-01"))
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.min_salary") >= 62000)
    .select(F.col("d.budget"), F.col("dt.min_salary"))
    .orderBy(F.col("budget").desc())
)
