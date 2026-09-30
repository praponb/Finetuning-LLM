from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Engineering", "Legal"]))
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
