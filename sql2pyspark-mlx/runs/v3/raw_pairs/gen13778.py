from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.min_salary") >= 82000)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.min_hire_date"),
        F.col("ds.min_salary"),
    )
)
