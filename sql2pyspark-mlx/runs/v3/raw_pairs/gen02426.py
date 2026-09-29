from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.location"))
    .agg(
        F.avg(F.col("e.salary")).alias("avg_salary"),
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
    )
    .orderBy("location")
)
