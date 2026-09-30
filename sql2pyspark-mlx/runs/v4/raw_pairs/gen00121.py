from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget").between(1291000, 4000000))
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.salary")).alias("max_salary"),
        F.sum(F.col("e.salary")).alias("total_salary"),
    )
    .orderBy(F.col("max_salary").desc(), "location")
    .limit(20)
)
