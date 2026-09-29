from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .select(
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("department_totals.salary_range"),
    )
    .orderBy(F.col("dept_name").desc())
)
