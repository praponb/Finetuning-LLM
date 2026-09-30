from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .filter(F.col("department_totals.avg_salary") >= 32)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("department_totals.last_hire_date"),
        F.col("department_totals.avg_salary"),
    )
)
