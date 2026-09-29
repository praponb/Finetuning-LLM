from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(~F.col("mgr.level").isin(["L5", "L1", "L3"]) & F.upper(F.col("mgr.email")).like("%ARO%"))
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
