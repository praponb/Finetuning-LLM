from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("e.is_remote"))
    .groupBy(F.col("d.location"))
    .agg(
        F.min(F.col("e.salary")).alias("min_salary"),
        F.count("*").alias("employee_count"),
    )
    .orderBy(F.col("min_salary").desc())
)
