from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.job_title") != "Manager")
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.when(F.col("e.job_title") == "Director", 1).otherwise(0)).alias("director_count"),
        F.countDistinct(F.col("e.salary")).alias("unique_levels"),
    )
    .orderBy(F.col("location").desc())
    .limit(1)
)
