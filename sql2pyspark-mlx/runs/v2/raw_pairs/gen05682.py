from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
    .orderBy("manager_dept_id")
)
