from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.level").isin(["L2", "L1"]))
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
    .orderBy(F.col("employee_email").desc())
)
