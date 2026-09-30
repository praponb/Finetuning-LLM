from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("d.budget") <= 1716000)
    .groupBy(F.col("d.location"))
    .agg(
        F.countDistinct(F.col("e.salary")).alias("unique_levels"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
    )
)
