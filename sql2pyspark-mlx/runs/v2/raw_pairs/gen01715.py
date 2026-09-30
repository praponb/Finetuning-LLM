from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("department_summary.min_hire_date"),
        F.col("department_summary.last_hire_date"),
    )
    .orderBy(F.col("budget").desc())
)
