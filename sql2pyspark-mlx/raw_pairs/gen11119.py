from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.is_remote") & F.col("m.level").isin(["L2", "L1", "L5"]))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.salary").alias("manager_salary"),
    )
)
