from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.manager_id").isin([6, 21, 29]))
    .select(
        F.col("emp.id"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("id").desc())
)
