from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("e.level").isin(["L3", "L2", "L1"]))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.email").alias("manager_email"),
    )
)
