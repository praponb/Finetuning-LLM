from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.salary").between(66000, 72000))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.salary").alias("manager_salary"),
    )
    .orderBy("employee_full_name")
)
