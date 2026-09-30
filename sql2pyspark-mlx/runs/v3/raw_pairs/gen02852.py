from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.level").isin(["L5", "L4"]))
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
)
