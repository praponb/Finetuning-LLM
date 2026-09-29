from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .cube(F.col("d.location").alias("location"), F.col("a.level").alias("level"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("a.salary")).alias("avg_salary"),
        F.grouping_id().alias("level_id"),
    )
)
