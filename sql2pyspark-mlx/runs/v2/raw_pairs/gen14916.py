from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.level") == "L4")
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
