from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.email").alias("manager_email"),
    )
)
