from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
