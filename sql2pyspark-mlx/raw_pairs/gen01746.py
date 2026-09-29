from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .groupingSets(
        [["employee_job_title", "employee_job_title"], ["employee_job_title"], []],
        F.col("p.job_title").alias("employee_job_title"),
        F.col("a.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.salary")).alias("total_salary"),
    )
)
