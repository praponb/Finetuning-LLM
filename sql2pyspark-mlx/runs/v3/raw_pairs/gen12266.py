from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        F.min(F.col("e.hire_date")).alias("min_hire_date"),
    )
)
