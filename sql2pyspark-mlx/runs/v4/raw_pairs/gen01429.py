from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .filter(F.col("department_totals.min_salary") > 62000)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("department_totals.min_salary"),
    )
)
