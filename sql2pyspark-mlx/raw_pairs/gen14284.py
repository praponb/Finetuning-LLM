from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "inner")
    .cube(
        F.col("p.job_title").alias("employee_job_title"),
        F.col("a.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.salary")).alias("max_salary"),
    )
    .orderBy(F.col("employee_job_title").desc_nulls_first())
)
