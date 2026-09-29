from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
    )
    .orderBy(F.col("location").desc())
    .limit(1)
)
