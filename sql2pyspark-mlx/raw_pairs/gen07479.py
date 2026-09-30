from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level").isNull())
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .select(
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("department_summary.first_hire_date"),
    )
)
