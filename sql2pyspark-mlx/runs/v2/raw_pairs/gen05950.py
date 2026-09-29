from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.upper(F.col("mgr.full_name")).like("%MMA%"))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("id").desc(), F.col("manager_salary").desc())
)
