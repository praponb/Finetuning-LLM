from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .rollup(
        F.col("e.level").alias("employee_level"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.salary")).alias("min_salary"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("employee_level").asc_nulls_first())
)
