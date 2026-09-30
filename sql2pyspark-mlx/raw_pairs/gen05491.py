from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date") > "2023-05-01")
    .groupBy(F.col("d.location"))
    .agg(
        F.avg(F.col("e.salary")).alias("avg_salary"),
        F.sum(F.when(F.col("e.level") == "L3", 1).otherwise(0)).alias("l3_count"),
    )
    .orderBy(F.col("l3_count").desc())
)
