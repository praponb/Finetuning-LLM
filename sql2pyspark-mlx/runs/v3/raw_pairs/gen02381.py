from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.count("*").alias("employee_count"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
    )
    .filter(F.col("employee_count") > 5)
    .orderBy(F.col("avg_salary").desc())
)
