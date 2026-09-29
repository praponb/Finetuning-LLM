from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy("manager_level", F.col("employee_dept_id").desc())
)
