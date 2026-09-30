from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter((F.col("mgr.manager_id") == 6) & (F.col("mgr.salary") < 128000))
    .select(
        F.col("emp.id"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.email").alias("manager_email"),
    )
)
