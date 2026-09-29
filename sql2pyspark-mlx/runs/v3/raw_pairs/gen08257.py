from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.length(F.col("emp.full_name")) >= 9)
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
