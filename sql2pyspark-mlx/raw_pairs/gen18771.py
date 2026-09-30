from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("department_totals.total_salary"),
    )
    .orderBy("id", F.col("total_salary").desc())
)
