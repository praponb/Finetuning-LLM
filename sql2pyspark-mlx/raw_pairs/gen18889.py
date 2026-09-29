from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.dept_id").isin([9, 10, 36]))
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
)
