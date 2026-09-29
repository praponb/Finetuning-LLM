from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name").isNotNull())
    .groupBy(F.col("d.location"))
    .agg(
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        F.round(F.stddev(F.col("e.salary")), 2).alias("stddev_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
