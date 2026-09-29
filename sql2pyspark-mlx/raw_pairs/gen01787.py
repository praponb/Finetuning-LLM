from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.job_title").alias("employee_job_title"),
        F.col("s.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.salary")), 2).alias("total_salary"),
    )
)
