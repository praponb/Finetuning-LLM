from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isin([5, 36]) & F.col("m.dept_id").isin([6, 15, 19]))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
