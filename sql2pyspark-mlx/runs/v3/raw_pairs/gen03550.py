from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
    .orderBy("employee_hire_date", F.col("manager_manager_id").desc(), "id")
    .limit(5)
)
