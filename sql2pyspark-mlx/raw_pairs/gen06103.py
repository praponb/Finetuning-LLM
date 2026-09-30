from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("dim"), F.col("a.manager_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.level").alias("employee_level"),
        F.col("a.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.salary")).alias("avg_salary"),
    )
)
