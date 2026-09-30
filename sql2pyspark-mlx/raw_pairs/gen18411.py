from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.email").like("%@yahoo.com"))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
    .filter(F.col("employee_count") >= 1)
)
