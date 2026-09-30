from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.level").isin(["L5", "L1"]) & F.col("emp.manager_id").isin([6, 18, 32]))
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
    )
)
