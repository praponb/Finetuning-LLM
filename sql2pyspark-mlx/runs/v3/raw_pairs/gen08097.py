from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("hire_date") < "2024-01-01")
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.avg_salary"),
        F.col("ds.salary_range"),
    )
)
