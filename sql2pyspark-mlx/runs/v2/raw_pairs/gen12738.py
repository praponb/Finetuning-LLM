from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
