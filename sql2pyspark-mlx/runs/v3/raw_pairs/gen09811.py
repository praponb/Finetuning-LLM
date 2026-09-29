from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.full_name").isNotNull())
    .select(
        F.col("emp.id"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
