from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.dept_id").isin([7, 30, 34]) & F.col("emp.job_title").isin(["Engineer", "Analyst", "Intern"]))
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
    .orderBy("employee_full_name", "id")
    .limit(50)
)
