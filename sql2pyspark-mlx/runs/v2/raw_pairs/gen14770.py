from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.is_remote"))
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.lower(F.col("emp.email")).alias("email_lower"),
    )
)
