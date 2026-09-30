from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.full_name").like("I%"))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        F.sum(F.when(F.col("e.level") == "L3", F.col("e.salary")).otherwise(0)).alias("l3_salary"),
    )
    .orderBy(F.col("employee_count").desc())
)
