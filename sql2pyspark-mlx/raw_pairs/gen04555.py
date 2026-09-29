from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "inner")
    .cube(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.salary")).alias("total_salary"),
    )
    .orderBy(F.col("total_salary").asc_nulls_first())
)
