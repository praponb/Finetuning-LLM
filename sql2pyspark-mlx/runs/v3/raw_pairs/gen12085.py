from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
