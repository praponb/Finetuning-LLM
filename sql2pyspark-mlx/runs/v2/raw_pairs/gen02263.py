from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy("id")
)
