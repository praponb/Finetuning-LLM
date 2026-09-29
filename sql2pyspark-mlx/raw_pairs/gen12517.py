from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.hire_date") > "2025-01-01")
    .select(
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
    .orderBy("manager_manager_id")
)
