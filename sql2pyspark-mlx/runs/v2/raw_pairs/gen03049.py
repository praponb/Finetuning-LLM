from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.location"))
    .agg(
        F.min(F.col("e.salary")).alias("min_salary"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
)
