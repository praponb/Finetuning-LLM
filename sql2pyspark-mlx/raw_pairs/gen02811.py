from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.dept_name").alias("dept_name"),
        F.col("d.location").alias("location"),
        F.col("s.level").alias("level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("s.salary")).alias("total_salary"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("location").asc_nulls_last())
)
