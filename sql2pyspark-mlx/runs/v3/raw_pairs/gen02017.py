from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.round(F.stddev(F.col("e.salary")), 2).alias("stddev_salary"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
    )
)
